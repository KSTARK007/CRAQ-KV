import glob
import os
import sys

# Function to read integers from a file
def read_integers(file_path):
    with open(file_path, 'r') as file:
        integers = set(map(int, file.read().splitlines()))
    return integers

# Check if the folder path is provided as an argument
if len(sys.argv) < 2:
    print("Please provide the folder path as an argument.")
    sys.exit(1)

# Get the folder path from the command-line argument
folder_path = sys.argv[1]

# Change the current directory to the specified folder path
os.chdir(folder_path)

# Extract all the files with the name cache_dump*.txt
file_paths = glob.glob('cache_dump*.txt')

# Read integers from each file
integer_sets = [read_integers(file_path) for file_path in file_paths]

# Calculate the union of all the keys
union_keys = set().union(*integer_sets)

# Calculate the intersection of all the keys
intersection_keys = set(integer_sets[0]).intersection(*integer_sets[1:])

# Calculate data coverage
data_coverage = len(union_keys) / 100000

# Calculate similarity
similarity = len(intersection_keys) / 100000

# Calculate Sorensen similarity
sorensen_similarity = (2 * len(intersection_keys)) / (sum(len(s) for s in integer_sets))

# Print the metrics
print(f"Data Coverage: {data_coverage}")
print(f"Similarity: {similarity}")
print(f"Sorensen Similarity: {sorensen_similarity}")

# Create the output directory if it doesn't exist
os.makedirs(folder_path, exist_ok=True)

# Write the keys that are in all three nodes to a file
output_file = os.path.join(folder_path, 'common_keys.txt')
with open(output_file, 'w') as file:
    for key in intersection_keys:
        file.write(str(key) + '\n')

print(f"Keys that are in all three nodes have been written to {output_file}.")