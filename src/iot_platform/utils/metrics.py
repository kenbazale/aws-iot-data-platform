from dataclasses import dataclass

@dataclass
class PipelineMetrics:
    """
    Metrics captured during a pipeline run.
    """
    
    input_records: int = 0
    valid_records: int = 0
    invalid_records: int = 0
    duplicate_records: int = 0
    
    @property
    def validation_rate(self) -> float:
        """Percentage of records that passed validation."""
        
        if self.input_records == 0:
            return 0.0
        
        return (
            self.valid_records / self.input_records
        ) * 100
    
    def log_summary(self, logger) -> None:
        """Write a pipeline summary to the configured logger."""

        logger.info(
            "Pipeline metrics | "
            "input=%d | "
            "valid=%d | "
            "invalid=%d | "
            "duplicates=%d | "
            "validation_rate=%.2f%%",
            self.input_records,
            self.valid_records,
            self.invalid_records,
            self.duplicate_records,
            self.validation_rate,
        )