"""Abstract base class for FITS file handling across instruments.

Provides a common interface for extracting metadata from FITS files
across different instruments (SDO, LASCO, SECCHI). Each instrument
module can implement this interface to standardize validation and
metadata extraction.

Classes:
    FITSHandler: Abstract base class for instrument-specific handlers.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from .result import ValidationResult


class FITSHandler(ABC):
    """Base class for instrument-specific FITS file handlers.

    Provides a common interface for extracting metadata from FITS files
    across different instruments (SDO, LASCO, SECCHI).

    Subclasses must implement:
        - extract_metadata: Open FITS, read header, parse datetime, return result.
        - get_save_dir: Determine target directory for a validated file.
        - to_db_record: Convert extracted metadata to a database-ready dict.
    """

    @abstractmethod
    def extract_metadata(self, file_path: str) -> ValidationResult:
        """Extract metadata from a FITS file.

        Opens the FITS file, reads the header, parses datetime and
        instrument-specific fields, and returns a structured result.

        Args:
            file_path: Path to the FITS file.

        Returns:
            ValidationResult with metadata on success, error on failure.
        """
        ...

    @abstractmethod
    def get_save_dir(self, download_root: str, **kwargs) -> Path:
        """Get the target save directory for a validated file.

        Determines the appropriate directory structure based on instrument,
        observation time, and other parameters.

        Args:
            download_root: Root download directory.
            **kwargs: Instrument-specific parameters (e.g., telescope,
                channel, datetime, camera, spacecraft).

        Returns:
            Path to target directory.
        """
        ...

    @abstractmethod
    def to_db_record(self, file_path: str, metadata: dict[str, Any]) -> dict[str, Any]:
        """Convert metadata to a database record dictionary.

        Transforms extracted FITS metadata into a flat dictionary matching
        the database table schema for the instrument.

        Args:
            file_path: Path to the FITS file.
            metadata: Extracted metadata dictionary from extract_metadata().

        Returns:
            Dictionary ready for database insertion.
        """
        ...
