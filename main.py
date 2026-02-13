from data_processing import DataProcessing
import sys
import json
import os

# ========== LOAD CONFIGURATION ==========

def load_config(config_file='datasets_config.json'):
    """Load dataset configurations from JSON file"""
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f" Error: Config file '{config_file}' not found")
        return {}
    except json.JSONDecodeError as e:
        print(f" Error: Invalid JSON in config file - {e}")
        return {}

# ========== PIPELINE FUNCTION ==========

def process_data(input_file, output_file, 
                 output_dir=None,
                 output_format=None,          # ← NEW: Optional format override
                 compression=None,            # ← NEW: Compression for Parquet
                 primary_column=None, 
                 sub_columns=None,
                 sort_order=None,
                 filter_column=None, 
                 filter_values=None,
                 keep_column=None,
                 keep_values=None,
                 show_stats=True):
    """
    Generic data processing pipeline for any dataset
    
    Args:
        input_file (str): Path to input file (format auto-detected)
        output_file (str): Path to save processed file (format auto-detected)
        output_dir (str, optional): Directory to save output files
        output_format (str, optional): Force output format ('csv', 'parquet', 'json', etc.)
        compression (str, optional): Compression for Parquet ('snappy', 'gzip', 'brotli')
        primary_column (str, optional): Column for primary clustering
        sub_columns (list, optional): List of columns for sub-clustering
        sort_order (list, optional): Custom order for sorting primary_column
        filter_column (str, optional): Column to filter
        filter_values (list, optional): Values to remove from filter_column
        keep_column (str, optional): Column to keep specific values from
        keep_values (list, optional): Values to keep in keep_column
        show_stats (bool): Whether to print statistics
    
    Returns:
        DataProcessing: Processed data object with clusters
    """
    
    # Create output directory if specified
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, output_file)
    else:
        output_path = output_file
    
    # Step 1: Load data (format auto-detected!)
    print(f" Loading data from {input_file}...")
    dp = DataProcessing(input_file)  # ← Format detected automatically
    print(f"   Loaded {len(dp.df):,} records\n")
    
    # Step 2: Apply filters (if specified)
    if filter_column and filter_values:
        print(f" Filtering out {filter_values} from '{filter_column}'...")
        dp.filter_by_column(filter_column, filter_values)
        print(f"   {len(dp.df):,} records remaining\n")
    
    # Step 3: Keep only specific values (if specified)
    if keep_column and keep_values:
        print(f" Keeping only {keep_values} in '{keep_column}'...")
        dp.keep_only_values(keep_column, keep_values)
        print(f"   {len(dp.df):,} records remaining\n")
    
    # Step 4: Sort by custom order (if specified)
    if primary_column and sort_order:
        print(f" Sorting by '{primary_column}' with custom order...")
        dp.sort_by_custom_order(primary_column, sort_order)
        print(f"   Sorted successfully\n")
    
    # Step 5: Create primary clusters (if specified)
    if primary_column:
        print(f" Creating clusters by '{primary_column}'...")
        dp.create_clusters(primary_column)
        
        # Step 6: Create sub-clusters (if specified)
        if sub_columns:
            for sub_col in sub_columns:
                print(f"   Creating sub-clusters by '{sub_col}'...")
                dp.create_sub_clusters(primary_column, sub_col)
        print()
    
    # Step 7: Print statistics (if requested)
    if show_stats and primary_column:
        print("="*60)
        print(" STATISTICS")
        print("="*60)
        
        # Show primary cluster stats
        for cluster_name, cluster_data in dp.clusters[primary_column].items():
            print(f"{cluster_name}: {len(cluster_data):,} records")
        
        # Show first sub-cluster example (if exists)
        if sub_columns:
            first_sub_col = sub_columns[0]
            key = f"{primary_column}_{first_sub_col}"
            if key in dp.sub_clusters:
                first_primary = list(dp.clusters[primary_column].keys())[0]
                first_sub = list(dp.sub_clusters[key][first_primary].keys())[0]
                sub_data = dp.get_sub_cluster(primary_column, first_sub_col, 
                                             first_primary, first_sub)
                if sub_data is not None:
                    print(f"\nExample: {first_primary} → {first_sub}: {len(sub_data):,} records")
        
        print("="*60 + "\n")
    
    # Step 8: Save processed data
    print(f" Saving processed data to {output_path}...")
    
    # Build kwargs for format-specific options
    save_kwargs = {}
    if compression:
        save_kwargs['compression'] = compression
    
    # Save with auto-detected or forced format
    dp.save_ordered_data(output_path, format=output_format, **save_kwargs)
    
    print(f" Processing complete!\n")

    return dp


def process_all_datasets(config_file='datasets_config.json'):
    """Process all datasets defined in config file"""
    configs = load_config(config_file)
    results = {}
    
    print(f" Processing {len(configs)} datasets...\n")
    print("="*60)
    
    for dataset_name, config in configs.items():
        print(f"\n📊 Processing: {dataset_name}")
        print("="*60)
        
        try:
            dp = process_data(**config)
            results[dataset_name] = dp
            print(f" {dataset_name} completed successfully\n")
        except Exception as e:
            print(f" Error processing {dataset_name}: {e}\n")
            results[dataset_name] = None
    
    print("="*60)
    print(f" Batch processing complete! Processed {len(configs)} datasets")
    
    return results


def process_single_dataset(dataset_name, config_file='datasets_config.json'):
    """Process a single dataset by name"""
    configs = load_config(config_file)
    
    if dataset_name not in configs:
        print(f"Error: Dataset '{dataset_name}' not found in config")
        print(f"Available datasets: {list(configs.keys())}")
        return None
    
    print(f" Processing: {dataset_name}\n")
    config = configs[dataset_name]
    return process_data(**config)


# ========== MAIN EXECUTION ==========

if __name__ == "__main__":
    # Load all configurations
    configs = load_config()
    
    if len(sys.argv) >= 2:
        # Command line mode - process specific dataset or special command
        command = sys.argv[1]
        
        if command == 'all':
            # Process all datasets
            print(f"🖥️  Command line mode: Processing ALL datasets\n")
            results = process_all_datasets()
            
        elif command == 'list':
            # List available datasets
            print("\n Available datasets:")
            for i, name in enumerate(configs.keys(), 1):
                config = configs[name]
                output_file = config.get('output_file', 'N/A')
                print(f"  {i}. {name}")
                print(f"     Output: {output_file}")
            print()
            
        else:
            # Process single dataset
            print(f"  Command line mode: {command}\n")
            dp = process_single_dataset(command)
        
    else:
        # Interactive mode - show usage
        print(" Python Data Pipeline")
        print("="*60)
        print("\n Available datasets:")
        for i, name in enumerate(configs.keys(), 1):
            config = configs[name]
            output_file = config.get('output_file', 'N/A')
            print(f"  {i}. {name} → {output_file}")
        
        print("\n Usage:")
        print("  python main.py <dataset_name>  - Process single dataset")
        print("  python main.py all             - Process all datasets")
        print("  python main.py list            - List all datasets")
        print("\nExamples:")
        print("  python main.py property_types_parquet")
        print("  python main.py all")
        print()
