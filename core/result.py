"""Type-safe result types for FITS validation and metadata extraction.

Replaces the previous pattern where validate_fits() returned dict on success
and str on failure, which was error-prone and not type-safe.

Classes:
    ValidationResult: Structured result for FITS file validation.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional


@dataclass
class ValidationResult:
    """Result of FITS file validation and metadata extraction.

    Replaces the dict|str return pattern with a structured, type-safe result.
    Callers check result.success instead of isinstance(result, str).

    Attributes:
        success: Whether validation succeeded.
        metadata: Extracted metadata dict on success, None on failure.
        error: Error category string on failure (e.g., 'invalid_file',
            'invalid_header', 'invalid_data', 'non_zero_quality'), None on success.
        file_path: Path to the validated file.

    Example:
        >>> result = validate_fits("path/to/file.fits")
        >>> if result.success:
        ...     print(result.metadata['datetime'])
        ... else:
        ...     print(f"Validation failed: {result.error}")
    """
    success: bool
    metadata: Optional[dict[str, Any]] = None
    error: Optional[str] = None
    file_path: Optional[str] = None

    @classmethod
    def ok(cls, metadata: dict[str, Any], file_path: str = None) -> 'ValidationResult':
        """Create a successful validation result.

        Args:
            metadata: Extracted metadata dictionary.
            file_path: Path to the validated file.

        Returns:
            ValidationResult with success=True.
        """
        return cls(success=True, metadata=metadata, file_path=file_path)

    @classmethod
    def fail(cls, error: str, file_path: str = None) -> 'ValidationResult':
        """Create a failed validation result.

        Args:
            error: Error category string.
            file_path: Path to the file that failed validation.

        Returns:
            ValidationResult with success=False.
        """
        return cls(success=False, error=error, file_path=file_path)
