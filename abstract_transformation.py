from abc import ABC, abstractmethod
import traceback
from typing import cast
from pyspark.sql.functions import col, DataFrame
from config_models.collection_config import Collection
from config_models.transformation_config import TransformationConfig
from pyspark.sql.functions import DataFrame, col
from transformations.table_writer import write_table
from utils.logger.exception_logger import ExceptionLogger
from utils.logger.event_logger import EventLogger
from utils.props_resolver import resolve_collection, resolve_target, get_job_context_parameters

class AbstractTransformation(ABC):
    def __init__(
        self,
        spark,
        config_file: str,
        target=None,
        collection: Collection | None = None,
        event_logger: None | EventLogger = None,
    ):
        """
        Base class for transformations.
        Args:
            spark (_type_): _spark instance_
            config_file (str): _Path to transformation config file e.g. "individual/aria_classify/ethnicity"_
            target (_type_, optional): _Target environment e.g. "uat"_. Defaults to None.
            collection (Collection enum value, optional): _the collection e.g. Collection.social_census_. Default reads collection notebook widget.
            event_logger (EventLogger, optional): For tests to inject mock logger. Will be removed if events are recorded as paradata
        """
        self.spark = spark
        self.target = target or resolve_target()
        self.collection = collection or resolve_collection()
        self.event_logger = event_logger

        self._transform_config = TransformationConfig(
            collection=self.collection, environment=self.target, transformation=config_file
        )

    def read(self) -> dict[str, DataFrame]:
        """_summary_
        Reads source tables into dict which it returns
        Returns:
            dict: The dictionary with source tables
        """
        data = dict()
        for source in self._transform_config.input_datasets:
            df = self.spark.table(source.path)
            if not source.include_all_variables:
                cols = [col(v.alias).alias(v.name) for v in source.variables]
                df = df.select(*cols)
            data[source.key] = df
        return data

    @abstractmethod
    def transform(self, data: dict[str, DataFrame]) -> DataFrame:
        """Apply business transformations"""
        pass

    def write(self, df: DataFrame) -> None:
        for output_dataset in self._transform_config.output_datasets:
            write_table(
                df=df,
                output_dataset=output_dataset,
                spark=self.spark,
            )

    def run(self) -> None:
        try:
            df = self.read()
            df_result = self.transform(df)
            self.write(df_result)
        except Exception as e:
            log_table = self._transform_config.collection_config.get_table_path(
                "exceptions", "logs"
            )
            job_context = get_job_context_parameters()
            with ExceptionLogger.for_task(
                spark=self.spark,
                log_table_path=log_table,
                run_id=job_context["job_run_id"],
                task_key=job_context["task_key"],
            ) as logger:
                logger = cast(ExceptionLogger, logger)
                logger.log(
                    rule="logic_conflict",
                    event_type="EXCEPTION",
                    variable_name=self._transform_config.name,
                    source_table=self._transform_config.input_datasets[0].path,
                    target_table=self._transform_config.output_datasets[0].path,
                    message=str(e),
                    stack_trace=traceback.format_exc(),
                )
                raise
