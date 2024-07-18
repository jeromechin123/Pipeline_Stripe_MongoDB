import subprocess

# List of Python files to run
file_list = ['C:\\Users\\jerom\\Winlads Stripe\\Pipeline\\stripe1_import.py', 
            'C:\\Users\\jerom\\Winlads Stripe\\Pipeline\\stripe2_import.py', 
            'C:\\Users\\jerom\\Winlads Stripe\\Pipeline\\stripe1_subscriptions_import.py',
            'C:\\Users\\jerom\\Winlads Stripe\\Pipeline\\query_landing_zone_export_curated_data.py',
            'C:\\Users\\jerom\\Winlads Stripe\\Pipeline\\stripe1_subscriptions_clean_mongodb.py',
            'C:\\Users\\jerom\\Winlads Stripe\\Pipeline\\stripe1_charges_clean_mongodb.py',
            'C:\\Users\\jerom\\Winlads Stripe\\Pipeline\\stripe2_charges_clean_mongodb.py'
            ]

# Function to run a Python file and check if it passes or fails
def run_file(file_name):
    print(f"Running {file_name}...")
    try:
        subprocess.run(['python', file_name], shell = True )
        print(f"{file_name} passed!")
    except subprocess.CalledProcessError:
        print(f"{file_name} failed!")

# Run each file in the list
for file in file_list:
    run_file(file)

print("All files have been run!")