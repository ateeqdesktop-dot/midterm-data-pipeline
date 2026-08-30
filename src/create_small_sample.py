import argparse
import csv
import os
import sys

def create_sample(input_path, output_path, target_rows):
    """Creates a sample CSV file by streaming from the input file."""
    if not os.path.exists(input_path):
        print(f"Error: Input file '{input_path}' does not exist.")
        sys.exit(1)
        
    print(f"Reading from {input_path} and writing {target_rows} rows to {output_path}...")
    
    try:
        with open(input_path, 'r', encoding='utf-8-sig', errors='replace') as infile:
            reader = csv.reader(infile)
            
            # Read header
            try:
                header = next(reader)
            except StopIteration:
                print("Error: Input file is empty.")
                sys.exit(1)
                
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8', newline='') as outfile:
                writer = csv.writer(outfile)
                writer.writerow(header)
                
                count = 0
                for row in reader:
                    writer.writerow(row)
                    count += 1
                    if count >= target_rows:
                        break
                        
        print(f"Success: Sample created with {count} rows at '{output_path}'.")
    except Exception as e:
        print(f"Error during sampling: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Create a small reproducible CSV sample from a large CSV file.")
    parser.add_argument("--input", default="data/orders_huge_mixed_quality.csv", help="Path to large input CSV file")
    parser.add_argument("--output", default="data/sample_orders.csv", help="Path to save the small sample CSV file")
    parser.add_argument("--rows", type=int, default=10000, help="Number of rows to sample (default: 10000)")
    
    args = parser.parse_args()
    create_sample(args.input, args.output, args.rows)

if __name__ == "__main__":
    main()
