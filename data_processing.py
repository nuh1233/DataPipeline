import pandas as pd
import os


class DataProcessing:
    
    def __init__(self, filename, file_format=None):
        """
        Initialize with automatic format detection
        
        Args:
            filename (str): Path to input file
            file_format (str, optional): Force specific format ('csv', 'parquet', 'json', 'excel')
                                        If None, will auto-detect from file extension
        """
        
        # Auto-detect format if not specified
        if file_format is None:
            file_format = self._detect_format(filename)
            print(f" Auto-detected format: {file_format}")
        
        # Validate file exists
        if not os.path.exists(filename):
            raise FileNotFoundError(f"File not found: {filename}")
        
        # Load based on detected/specified format
        print(f" Loading {file_format.upper()} file: {filename}")
        
        if file_format == 'csv':
            self.df = pd.read_csv(filename, low_memory=False)
        
        elif file_format == 'parquet':
            self.df = pd.read_parquet(filename)
        
        elif file_format == 'json':
            self.df = pd.read_json(filename)
        
        elif file_format in ['excel', 'xlsx', 'xls']:
            self.df = pd.read_excel(filename)
        
        elif file_format == 'feather':
            self.df = pd.read_feather(filename)
        
        elif file_format == 'hdf':
            self.df = pd.read_hdf(filename)
        
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
        
        print(f" Loaded {len(self.df):,} records, {len(self.df.columns)} columns\n")
        
        self.clusters = {}
        self.sub_clusters = {}
        self.schema = None

    @classmethod
    def from_dataframe(cls, df):
        """
        Alternate constructor that initializes DataProcessing directly from a pandas DataFrame.

        Args:
            df (pd.DataFrame): Pre-loaded DataFrame to manipulate and save.

        Returns:
            DataProcessing: Instance with df set and ready for manipulation APIs.
        """
        instance = object.__new__(cls)
        # Bypass __init__ file loading; set attributes directly
        instance.df = df.copy()
        instance.clusters = {}
        instance.sub_clusters = {}
        instance.schema = None
        print(f" Initialized from DataFrame: {len(instance.df):,} records, {len(instance.df.columns)} columns\n")
        return instance
    
    def _detect_format(self, filename):
        """
        Automatically detect file format from extension
        
        Args:
            filename (str): Path to file
        
        Returns:
            str: Detected format ('csv', 'parquet', 'json', etc.)
        """
        # Get file extension (lowercase, without the dot)
        ext = os.path.splitext(filename)[1].lower().lstrip('.')
        
        # Map extensions to formats
        format_map = {
            'csv': 'csv',
            'parquet': 'parquet',
            'pq': 'parquet',
            'json': 'json',
            'jsonl': 'json',
            'xlsx': 'excel',
            'xls': 'excel',
            'feather': 'feather',
            'ftr': 'feather',
            'h5': 'hdf',
            'hdf': 'hdf',
            'hdf5': 'hdf'
        }
        
        detected = format_map.get(ext)
        
        if detected is None:
            raise ValueError(
                f"Could not detect format for extension '.{ext}'. "
                f"Supported formats: {list(format_map.values())}"
            )
        
        return detected
    
    def sort_by_custom_order(self, column_name, category_order):
        """Sort data by custom category order"""
        self.df[column_name] = self.df[column_name].str.title()
        
        self.df[column_name] = pd.Categorical(
            self.df[column_name],
            categories=category_order,
            ordered=True
        )

        self.df = self.df.sort_values(by=column_name, na_position='last')
        return self.df

    def create_clusters(self, column_name):
        """Create primary clusters for fast retrieval"""
        self.clusters[column_name] = {
            name: group for name, group in self.df.groupby(column_name)
        }
        
        print(f"Created {len(self.clusters[column_name])} clusters for '{column_name}'")
        return self.clusters[column_name]

    def create_sub_clusters(self, primary_column, sub_column):
        """Create sub-clusters within each primary cluster"""
        if primary_column not in self.clusters:
            self.create_clusters(primary_column)
        
        self.sub_clusters[f"{primary_column}_{sub_column}"] = {}
        
        for primary_value, primary_group in self.clusters[primary_column].items():
            sub_groups = {
                sub_value: sub_group 
                for sub_value, sub_group in primary_group.groupby(sub_column)
            }
            
            self.sub_clusters[f"{primary_column}_{sub_column}"][primary_value] = sub_groups
            print(f"  {primary_value}: {len(sub_groups)} sub-clusters")
        
        return self.sub_clusters[f"{primary_column}_{sub_column}"]

    def get_sub_cluster(self, primary_column, sub_column, primary_value, sub_value):
        """Retrieve a specific sub-cluster"""
        key = f"{primary_column}_{sub_column}"
        
        if key not in self.sub_clusters:
            self.create_sub_clusters(primary_column, sub_column)
        
        try:
            return self.sub_clusters[key][primary_value][sub_value]
        except KeyError:
            print(f"No data found for {primary_value} -> {sub_value}")
            return None
    
    def filter_by_column(self, column_name, values_to_drop):
        """Filter out rows where column value is in the specified list"""
        initial_count = len(self.df)
        self.df = self.df[~self.df[column_name].isin(values_to_drop)]
        dropped_count = initial_count - len(self.df)
        print(f"Dropped {dropped_count} rows where {column_name} was in {values_to_drop}")
        return self.df

    def keep_only_values(self, column_name, values_to_keep):
        """Keep only rows where column value is in the specified list"""
        initial_count = len(self.df)
        self.df = self.df[self.df[column_name].isin(values_to_keep)]
        dropped_count = initial_count - len(self.df)
        print(f"Kept only {values_to_keep} in {column_name}. Dropped {dropped_count} rows.")
        return self.df
    
    def save_ordered_data(self, output_filename, format=None, **kwargs):
        """
        Save processed data with auto-detected or specified format
        
        Args:
            output_filename (str): Output file path
            format (str, optional): Force specific output format. If None, auto-detect from extension
            **kwargs: Additional arguments passed to save function (compression, etc.)
        """
        # Auto-detect output format
        if format is None:
            format = self._detect_format(output_filename)
            print(f" Auto-detected output format: {format}")
        
        # Create output directory if needed
        output_dir = os.path.dirname(output_filename)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        
        print(f" Saving as {format.upper()}: {output_filename}")
        
        # Save based on format
        if format == 'csv':
            self.df.to_csv(output_filename, index=False, **kwargs)
        
        elif format == 'parquet':
            self.df.to_parquet(output_filename, index=False, **kwargs)
        
        elif format == 'json':
            self.df.to_json(output_filename, orient='records', indent=2, **kwargs)
        
        elif format in ['excel', 'xlsx', 'xls']:
            self.df.to_excel(output_filename, index=False, **kwargs)
        
        elif format == 'feather':
            self.df.to_feather(output_filename, **kwargs)
        
        elif format == 'hdf':
            self.df.to_hdf(output_filename, key='data', mode='w', **kwargs)
        
        else:
            raise ValueError(f"Unsupported output format: {format}")
        
        # Print file info
        file_size = os.path.getsize(output_filename) / (1024 * 1024)  # MB
        print(f" Saved successfully")
        print(f"   Size: {file_size:.2f} MB")
        print(f"   Rows: {len(self.df):,}")
        print(f"   Columns: {len(self.df.columns)}\n")
