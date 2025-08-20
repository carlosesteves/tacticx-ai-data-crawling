import csv

def write_csv(data, file_name):
    with open(file_name, 'w') as file:
        writer = csv.writer(file)

        # Ensure all rows are properly encoded
        for row in data:
            writer.writerow([s if isinstance(s, str) else str(s) for s in row])