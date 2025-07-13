import pandas as pd
import geofilters
import logging
from typing import Any
from pathlib import Path
from abc import ABC, abstractmethod
from pandas import DataFrame


class BaseExtractor(ABC):
    """
        Abstract extractor class which serves as a base
        for all other extractors.
    """

    def __init__(self, config: dict, file_config: dict, loader: object):
        """BaseExctractor constructor.

        Args:
            config (dict): Script configuration
            file_config (dict): Specific file configuration
        """
        self.config = config
        self.file_config = file_config
        self.loader = loader
        self.logger = logging.getLogger(__name__)

        if file_config["geofilter"] != None:
            self.geofilter = getattr(
                geofilters, file_config["geofilter"])(config, file_config)
        else:
            self.geofilter = None

        if file_config["date_config"] != None:
            self.date_columns = file_config["date_config"]["columns"]
        else:
            self.date_columns = []

    def extract_data(self, file_path: Path, processed_ids: DataFrame) -> DataFrame:
        """
            Extract data from provided file path and process it.

        Args:
            file_path (Path): Path to file
            processed_ids (DataFrame): IDs of entrise from other files
              which were processed before this one

        Returns:
            DataFrame: Extracted data from the file
        """

        file_data = self.load_file_data(file_path)
        if file_data.empty:
            self.logger.warning("No entries present in specified file")
            return file_data
        self.logger.debug(f"{len(file_data)} entries loaded")

        database_data = self.loader.load_processed_data()
        self.logger.debug(f"{len(database_data)} saved entries loaded")

        filtered_data = self.filter_data(
            file_data, database_data, processed_ids)
        if filtered_data.empty:
            self.logger.debug("No entries remaining after filtering")
            return filtered_data
        self.logger.debug(f"{len(filtered_data)} entries filtered")

        transformed_data = self.transform_data(filtered_data)
        if transformed_data.empty:
            self.logger.debug("No entries remaining after transformation")
            return transformed_data
        self.logger.debug(f"{len(transformed_data)} entries transformed")
        return transformed_data

    @abstractmethod
    def load_file_data(self, file_path: Path) -> DataFrame:
        """
            Load file data into a dataframe.

        Args:
            file_path (Path): Path to the file

        Returns:
            DataFrame: File data
        """
        raise NotImplementedError

    def process_point_poly_intersect(self, in_df: DataFrame, polygon) -> tuple[list[pd.Series], list[pd.Series]]:
        """
            Split entries into two lists - those which intersect with the provided
            polygon and those which have swapped coordinates.

        Args:
            in_df (DataFrame): Dataframe containing data and geometry information.
            polygon: Polygon with which the data will be intersected.

        Returns:
            tuple[list[pd.Series], list[pd.Series]]: List of intersecting entries
              and of invalid entries.
        """

        # Check validity of the coordiantes
        valid_df, invalid_df = self.geofilter.point_xy_validation(in_df)
        if valid_df.empty:
            return valid_df, invalid_df

        # Intersect points with polygon
        valid_df = self.geofilter.point_poly_intersect(valid_df, polygon)
        return valid_df, invalid_df

    def filter_data(self, file_data: DataFrame, database_data: DataFrame, processed_ids: DataFrame) -> DataFrame:
        """
            Filter extracted data to process only those which were processed earlier.
            Spatial data must intersect with specified polygon area.

        Args:
            file_data (DataFrame): Data extracted form the file
            database_data (DataFrame): Existing data in the database
            processed_ids (DataFrame): IDs which were processed previously

        Returns:
            DataFrame: Filtered data
        """

        # Spatial data should have coordiantes entry specified
        if self.file_config["coordinates"] != None:
            # Ignore entries which are already present in the database
            if not database_data.empty:
                if self.file_config["id_column"] in database_data.columns:
                    filtered_data = file_data[~file_data[self.file_config["id_column"]].isin(
                        database_data[self.file_config["id_column"]])]
                    self.logger.debug(
                        f"{len(file_data) - len(filtered_data)} already present.")
                    file_data = filtered_data
                else:
                    self.logger.warning(
                        f"Column '{self.file_config['id_column']}' not found in processed data. All data will be processed as none can be filtered out.")

            polygon = self.geofilter.load_polygon_filter()
            self.logger.debug(f"Polygon loaded")

            renamed_data = file_data.rename(
                columns=self.file_config["coordinates"])
            self.logger.debug(f"Coordinate columns renamed")

            geo_df = self.geofilter.add_geometry(renamed_data)
            self.logger.debug(f"Geometry added to dataframe")

            valid_df, invalid_df = self.process_point_poly_intersect(
                geo_df, polygon)

            self.logger.debug(
                f"{len(valid_df)} valid entries, {len(invalid_df)} invalid entries")

            corrected_df = pd.DataFrame()
            if len(invalid_df) > 0:
                swapped_df = self.geofilter.swap_xy(invalid_df)

                # Ignore invalid results, coordinates are outside Czech republic
                corrected_df, _ = self.process_point_poly_intersect(
                    swapped_df, polygon)

                self.logger.debug(
                    f"{len(corrected_df)} swapped valid entries")

                # Merge results into single dataframe
                filtered_df = pd.concat([valid_df, corrected_df])
            else:
                filtered_df = valid_df

            self.logger.debug(f"{len(filtered_df)} valid entries overall")

            return filtered_df
        else:
            if not processed_ids.empty:
                file_data = file_data[file_data[self.file_config["id_column"]].isin(
                    processed_ids[self.file_config["id_column"]])]
                self.logger.debug(f"{len(file_data)} matching entries.")
            return file_data

    @abstractmethod
    def transform_data(self, filtered_data: DataFrame) -> DataFrame:
        """
            Transform filtered data.

        Args:
            filtered_data (DataFrame): Filtered data

        Returns:
            DataFrame: Transformed data
        """
        raise NotImplementedError


class CSVExtractor(BaseExtractor):
    """
        Extractor used for processing of CSV files.
    """

    def load_file_data(self, file_path: Path) -> DataFrame:
        """
            Load file data into a dataframe. Once the file was loaded
            it is then stored as a PKL file instead and the original file
            is removed. This greatly improves speed of subsequent runs.

        Args:
            file_path (Path): Path to the file

        Returns:
            DataFrame: File data
        """

        csv_path = file_path.with_suffix(".csv")
        pkl_path = file_path.with_suffix(".pkl")
        if Path(pkl_path).exists():
            df = pd.read_pickle(pkl_path)
        else:
            df = pd.read_csv(csv_path,
                             encoding=self.file_config["encoding"],
                             delimiter=self.file_config["delimiter"],
                             names=self.file_config["columns"],
                             parse_dates=self.date_columns,
                             decimal=self.file_config["decimal"],
                             low_memory=False)
            df.to_pickle(pkl_path)
            Path.unlink(csv_path)
        return df

    def transform_data(self, filtered_data: DataFrame) -> DataFrame:
        """
            Transform filtered data.

        Args:
            filtered_data (DataFrame): Filtered data

        Returns:
            DataFrame: Transformed data
        """

        if len(filtered_data.columns) == 0:
            return filtered_data

        transformed_data = filtered_data.dropna(how="all", axis="columns")
        self.logger.debug("Dropped unknown columns")

        if transformed_data.empty:
            return transformed_data

        drop_columns = [col for col in self.file_config["drop_columns"]
                        if col in transformed_data.columns]
        transformed_data = transformed_data.drop(drop_columns, axis=1)
        self.logger.debug("Dropped specified columns")

        for col in self.date_columns:
            transformed_data[col] = pd.to_datetime(
                transformed_data[col],
                format=self.file_config["date_config"]["in_format"]
            )
            transformed_data[col] = transformed_data[col].dt.strftime(
                self.file_config["date_config"]["out_format"]
            )
        self.logger.debug("Transformed date formats")

        return transformed_data


class XLSExtractor(BaseExtractor):
    """
        Extractor used for processing of XLS accident files.
    """

    def load_file_data(self, file_path: Path) -> DataFrame:
        """
            Load file data into a dataframe. Once the file was loaded
            it is then stored as a PKL file instead and the original file
            is removed. This greatly improves speed of subsequent runs.

        Args:
            file_path (Path): Path to the file

        Returns:
            DataFrame: File data
        """

        xls_path = file_path.with_suffix(".xls")
        pkl_path = file_path.with_suffix(".pkl")
        if Path(pkl_path).exists():
            df = pd.read_pickle(pkl_path)
        else:
            df = pd.read_html(xls_path,
                              flavor="lxml",
                              header=0,
                              parse_dates=True,
                              decimal=",")[0]
            df.to_pickle(pkl_path)
            Path.unlink(xls_path)
        return df

    def transform_data(self, filtered_data: DataFrame) -> DataFrame:
        """
            Transform filtered data.

        Args:
            filtered_data (DataFrame): Filtered data

        Returns:
            DataFrame: Transformed data
        """

        if len(filtered_data.columns) == 0:
            return filtered_data

        transformed_data = filtered_data.dropna(how="all", axis=1)
        self.logger.debug("Dropped unknown columns")

        transformed_data = transformed_data.dropna(how="all", axis=0)
        self.logger.debug("Dropped empty entries")

        if transformed_data.empty:
            return transformed_data

        drop_columns = [col for col in self.file_config["drop_columns"]
                        if col in transformed_data.columns]
        transformed_data = transformed_data.drop(drop_columns, axis=1)
        self.logger.debug("Dropped specified columns")

        rename_dict = {col: new_col for [col, new_col] in self.file_config["rename_columns"].items(
        ) if col in transformed_data.columns}
        transformed_data = transformed_data.rename(columns=rename_dict)
        self.logger.debug("Renamed specified columns")

        for col in self.date_columns:
            transformed_data[col] = pd.to_datetime(
                transformed_data[col],
                format=self.file_config["date_config"]["in_format"]
            )
            transformed_data[col] = transformed_data[col].dt.strftime(
                self.file_config["date_config"]["out_format"]
            )
        self.logger.debug("Transformed date formats")

        return transformed_data
