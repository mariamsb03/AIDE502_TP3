import os
import boto3
import pyarrow as pa
import pyarrow.parquet as pq


def unpack_data(input_dir, bucket_name, output_file_name):
    """
    Reads local Arrow files from train, test, and dev subfolders,
    combines them, and uploads the combined text file to the specified S3 bucket.

    Parameters:
    input_dir (str): Path to the directory containing the train, test, dev subfolders.
    bucket_name (str): Name of the S3 bucket to upload the combined file to.
    output_file_name (str): Name of the combined text file to be uploaded to S3.
    """
    s3 = boto3.client('s3', endpoint_url='http://localhost:4566')
    all_texts = []

    # Subfolders: train, test, dev
    subfolders = ['train', 'test', 'dev']

    # Iterate through subfolders
    for subfolder in subfolders:
        subfolder_path = os.path.join(input_dir, subfolder)
        
        if os.path.exists(subfolder_path) and os.path.isdir(subfolder_path):
            print(f"\nProcessing folder: {subfolder}")
            
            for file_name in os.listdir(subfolder_path):
                file_path = os.path.join(subfolder_path, file_name)
                
                # Skip if not a file
                if not os.path.isfile(file_path):
                    continue
                
                # Process Arrow files
                if file_name.endswith('.arrow'):
                    print(f"  Reading: {file_name}")
                    try:
                        # Try reading as Arrow IPC file format first
                        try:
                            with pa.memory_map(file_path, 'r') as source:
                                table = pa.ipc.RecordBatchFileReader(source).read_all()
                        except:
                            # If that fails, try streaming format
                            with pa.memory_map(file_path, 'r') as source:
                                reader = pa.ipc.open_stream(source)
                                table = reader.read_all()
                        
                        # Convert to pandas for easier text extraction
                        df = table.to_pandas()
                        
                        # Extract text column
                        if 'text' in df.columns:
                            texts = df['text'].tolist()
                            # Filter out empty texts
                            texts = [text.strip() for text in texts if text and text.strip()]
                            all_texts.extend(texts)
                            print(f"    ✓ Extracted {len(texts)} non-empty texts")
                        else:
                            print(f"    ✗ Warning: 'text' column not found")
                    except Exception as e:
                        print(f"    ✗ Error reading file: {e}")
                
                # Process Parquet files (if present)
                elif file_name.endswith('.parquet'):
                    print(f"  Reading: {file_name}")
                    try:
                        table = pq.read_table(file_path)
                        df = table.to_pandas()
                        
                        if 'text' in df.columns:
                            texts = df['text'].tolist()
                            texts = [text.strip() for text in texts if text and text.strip()]
                            all_texts.extend(texts)
                            print(f"    ✓ Extracted {len(texts)} non-empty texts")
                        else:
                            print(f"    ✗ Warning: 'text' column not found")
                    except Exception as e:
                        print(f"    ✗ Error reading file: {e}")
        else:
            print(f"\n⚠ Subfolder '{subfolder}' does not exist at {subfolder_path}")

    if all_texts:
        print(f"\nTotal texts collected: {len(all_texts)}")
        
        # Create a temporary directory if it doesn't exist
        temp_dir = "temp"
        os.makedirs(temp_dir, exist_ok=True)
        
        # Save the combined data to a text file
        combined_file_path = os.path.join(temp_dir, output_file_name)
        
        with open(combined_file_path, 'w', encoding='utf-8') as f:
            for text in all_texts:
                f.write(text + '\n')
        
        print(f"Combined file saved locally at {combined_file_path}")
        
        # Upload the combined file to the S3 bucket
        try:
            s3.upload_file(combined_file_path, bucket_name, output_file_name)
            print(f"Uploaded combined file to bucket '{bucket_name}' with name '{output_file_name}'")
        except Exception as e:
            print(f"Error uploading to S3: {e}")
            raise
        
        # Clean up local file
        if os.path.exists(combined_file_path):
            os.remove(combined_file_path)
            print("Cleaned up temporary file")
    else:
        print("No valid texts found to process.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Unpack WikiText Arrow files, combine, and upload to S3")
    parser.add_argument("--input_dir", type=str, required=True, help="Path to input directory")
    parser.add_argument("--bucket_name", type=str, required=True, help="Name of the S3 bucket")
    parser.add_argument("--output_file_name", type=str, default="combined_raw.txt", help="Name of the output file for S3")
    args = parser.parse_args()

    unpack_data(args.input_dir, args.bucket_name, args.output_file_name)