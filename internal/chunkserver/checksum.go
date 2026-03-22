package chunkserver

import (
	"fmt"
	"hash/crc32"
	"log"
)

// computeBlockChecksums computes 64KB block-level checksums for the given data
// as required by GFS paper Section 5.2
func computeBlockChecksums(data []byte) []BlockChecksum {
	var checksums []BlockChecksum

	for i := 0; i < len(data); i += CHECKSUM_BLOCK_SIZE {
		end := i + CHECKSUM_BLOCK_SIZE
		if end > len(data) {
			end = len(data)
		}

		block := data[i:end]
		checksum := crc32.ChecksumIEEE(block)

		checksums = append(checksums, BlockChecksum{
			BlockIndex: uint32(i / CHECKSUM_BLOCK_SIZE),
			Checksum:   checksum,
		})
	}

	return checksums
}

// verifyBlockChecksums verifies block-level checksums for a read operation
// Only verifies blocks that overlap with the read range for efficiency
func verifyBlockChecksums(data []byte, offset int64, length int64, storedChecksums []BlockChecksum) error {
	if len(storedChecksums) == 0 {
		// No checksums available - this might be an old chunk
		log.Printf("Warning: No block checksums available for verification")
		return nil
	}

	// Calculate which blocks overlap with the read range
	startBlock := offset / CHECKSUM_BLOCK_SIZE
	endBlock := (offset + length - 1) / CHECKSUM_BLOCK_SIZE

	// Create a map for quick lookup of stored checksums
	checksumMap := make(map[uint32]uint32)
	for _, bc := range storedChecksums {
		checksumMap[bc.BlockIndex] = bc.Checksum
	}

	// Verify each overlapping block
	for blockIdx := startBlock; blockIdx <= endBlock; blockIdx++ {
		blockStart := blockIdx * CHECKSUM_BLOCK_SIZE
		blockEnd := blockStart + CHECKSUM_BLOCK_SIZE

		// Determine the portion of this block that we need to verify
		verifyStart := blockStart
		verifyEnd := blockEnd

		if verifyStart < offset {
			verifyStart = offset
		}
		if verifyEnd > offset+length {
			verifyEnd = offset + length
		}

		// Read the entire block for checksum verification
		if blockEnd > int64(len(data)) {
			blockEnd = int64(len(data))
		}

		if blockStart >= int64(len(data)) {
			break // No more blocks to verify
		}

		block := data[blockStart:blockEnd]
		computedChecksum := crc32.ChecksumIEEE(block)

		// Compare with stored checksum
		if storedChecksum, exists := checksumMap[uint32(blockIdx)]; exists {
			if computedChecksum != storedChecksum {
				return fmt.Errorf("block %d checksum mismatch: computed=%x, stored=%x",
					blockIdx, computedChecksum, storedChecksum)
			}
		} else {
			log.Printf("Warning: No stored checksum for block %d", blockIdx)
		}
	}

	return nil
}

// updateBlockChecksumsForWrite updates block checksums when data is written at a specific offset
// This is used for random writes and overwrites
func updateBlockChecksumsForWrite(existingChecksums []BlockChecksum, writeData []byte,
	writeOffset int64, fullChunkData []byte) []BlockChecksum {

	// Recompute checksums for all blocks affected by the write
	affectedStartBlock := writeOffset / CHECKSUM_BLOCK_SIZE
	affectedEndBlock := (writeOffset + int64(len(writeData)) - 1) / CHECKSUM_BLOCK_SIZE

	// Create a map for quick lookup and update
	checksumMap := make(map[uint32]uint32)
	for _, bc := range existingChecksums {
		checksumMap[bc.BlockIndex] = bc.Checksum
	}

	// Recompute checksums for affected blocks
	for blockIdx := affectedStartBlock; blockIdx <= affectedEndBlock; blockIdx++ {
		blockStart := blockIdx * CHECKSUM_BLOCK_SIZE
		blockEnd := blockStart + CHECKSUM_BLOCK_SIZE

		if blockEnd > int64(len(fullChunkData)) {
			blockEnd = int64(len(fullChunkData))
		}

		if blockStart >= int64(len(fullChunkData)) {
			break
		}

		block := fullChunkData[blockStart:blockEnd]
		newChecksum := crc32.ChecksumIEEE(block)
		checksumMap[uint32(blockIdx)] = newChecksum
	}

	// Convert map back to slice
	var updatedChecksums []BlockChecksum
	for blockIdx, checksum := range checksumMap {
		updatedChecksums = append(updatedChecksums, BlockChecksum{
			BlockIndex: blockIdx,
			Checksum:   checksum,
		})
	}

	return updatedChecksums
}

// updateBlockChecksumsForAppend efficiently updates checksums for append operations
// GFS paper Section 5.2: "We just incrementally update the checksum for the last partial
// checksum block, and compute new checksums for any brand new checksum blocks"
func updateBlockChecksumsForAppend(existingChecksums []BlockChecksum, appendData []byte,
	appendOffset int64, newChunkData []byte) []BlockChecksum {

	// Find the last partial block (if any)
	lastPartialBlock := appendOffset / CHECKSUM_BLOCK_SIZE

	// Create map from existing checksums
	checksumMap := make(map[uint32]uint32)
	for _, bc := range existingChecksums {
		checksumMap[bc.BlockIndex] = bc.Checksum
	}

	// Check if we need to update the last partial block
	if appendOffset%CHECKSUM_BLOCK_SIZE != 0 {
		// Last block is partial - recompute its checksum
		blockStart := lastPartialBlock * CHECKSUM_BLOCK_SIZE
		blockEnd := blockStart + CHECKSUM_BLOCK_SIZE

		if blockEnd > int64(len(newChunkData)) {
			blockEnd = int64(len(newChunkData))
		}

		block := newChunkData[blockStart:blockEnd]
		newChecksum := crc32.ChecksumIEEE(block)
		checksumMap[uint32(lastPartialBlock)] = newChecksum
	}

	// Compute checksums for any new complete blocks
	firstNewBlock := (appendOffset + int64(len(appendData)) - 1) / CHECKSUM_BLOCK_SIZE
	if appendOffset%CHECKSUM_BLOCK_SIZE == 0 {
		firstNewBlock = appendOffset / CHECKSUM_BLOCK_SIZE
	} else {
		firstNewBlock = lastPartialBlock + 1
	}

	for blockIdx := firstNewBlock; blockIdx*CHECKSUM_BLOCK_SIZE < int64(len(newChunkData)); blockIdx++ {
		blockStart := blockIdx * CHECKSUM_BLOCK_SIZE
		blockEnd := blockStart + CHECKSUM_BLOCK_SIZE

		if blockEnd > int64(len(newChunkData)) {
			blockEnd = int64(len(newChunkData))
		}

		block := newChunkData[blockStart:blockEnd]
		checksum := crc32.ChecksumIEEE(block)
		checksumMap[uint32(blockIdx)] = checksum
	}

	// Convert back to slice
	var updatedChecksums []BlockChecksum
	for blockIdx, checksum := range checksumMap {
		updatedChecksums = append(updatedChecksums, BlockChecksum{
			BlockIndex: blockIdx,
			Checksum:   checksum,
		})
	}

	return updatedChecksums
}

// verifyBlockChecksumsOnRead verifies checksums for a read operation
// This is the main function called during ReadChunk operations
func verifyBlockChecksumsOnRead(chunkHandle string, readOffset int64, readLength int64,
	fullChunkData []byte, metadata *ChunkMetadata) error {

	if len(metadata.BlockChecksums) == 0 {
		// Fall back to legacy full-chunk checksum verification
		log.Printf("Chunk %s: Using legacy full-chunk checksum verification", chunkHandle)
		computedChecksum := crc32.ChecksumIEEE(fullChunkData)
		if computedChecksum != metadata.Checksum {
			return fmt.Errorf("legacy chunk checksum mismatch: computed=%x, stored=%x",
				computedChecksum, metadata.Checksum)
		}
		return nil
	}

	// Use block-level verification
	return verifyBlockChecksums(fullChunkData, readOffset, readLength, metadata.BlockChecksums)
}
