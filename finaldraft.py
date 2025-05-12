import csv
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import logging
import schedule
import time

# Configure logging
logging.basicConfig(filename='analysis_log.txt', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def load_data(file_path):
    """Load data from a CSV file."""
    try:
        df = pd.read_csv(file_path)
        logging.info("Data loaded successfully.")
        return df
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        raise

def process_data(df):
    """Clean and transform the data."""
    try:
        df["charges"] = df["charges"].astype(float)
        df["age"] = df["age"].astype(int)
        logging.info("Data processed successfully.")
        return df
    except Exception as e:
        logging.error(f"Error processing data: {e}")
        raise

def save_data(df, file_path):
    """Save processed data to a CSV file."""
    try:
        df.to_csv(file_path, index=False)
        logging.info(f"Processed data saved to {file_path}.")
    except Exception as e:
        logging.error(f"Error saving data: {e}")
        raise

class PatientsInfo:
    """Class to analyze patient data."""
    def __init__(self, df):
        self.df = df

    def analyze_ages(self):
        """Calculate the average age of patients."""
        return round(self.df["age"].mean(), 2)

    def analyze_sexes(self):
        """Count the number of males and females."""
        return self.df["sex"].value_counts().to_dict()

    def unique_regions(self):
        """Get unique regions."""
        return self.df["region"].unique().tolist()

    def average_charges(self):
        """Calculate the average insurance charges."""
        return round(self.df["charges"].mean(), 2)

    def create_dictionary(self):
        """Create a dictionary of patient data."""
        return self.df.to_dict(orient="list")

def visualize_data(df):
    """Create and display visualizations."""
    try:
        # Average charges by region
        plt.figure(figsize=(8, 5))
        sns.barplot(x=df.groupby("region")["charges"].mean().index,
                    y=df.groupby("region")["charges"].mean().values, palette="Blues")
        plt.xlabel("Region")
        plt.ylabel("Average Charges ($)")
        plt.title("Average Medical Insurance Charges by Region")
        plt.xticks(rotation=45)
        plt.show()

        # Average charges by smoker status
        plt.figure(figsize=(8, 5))
        sns.barplot(x=df.groupby("smoker")["charges"].mean().index,
                    y=df.groupby("smoker")["charges"].mean().values, palette="Set1")
        plt.xlabel("Smoker Status")
        plt.ylabel("Average Charges ($)")
        plt.title("Average Charges by Smoking Status")
        plt.show()
    except Exception as e:
        logging.error(f"Error creating visualizations: {e}")

def automated_analysis(df):
    """Perform automated analysis and log results."""
    patients_info = PatientsInfo(df)
    avg_age = patients_info.analyze_ages()
    sex_distribution = patients_info.analyze_sexes()
    avg_charges = patients_info.average_charges()
    unique_regions = patients_info.unique_regions()

    # Log the analysis results
    logging.info(f"Average Patient Age: {avg_age} years")
    logging.info(f"Sex Distribution: {sex_distribution}")
    logging.info(f"Average Yearly Charges: ${avg_charges}")
    logging.info(f"Unique Regions: {unique_regions}")

    # Visualize the data
    visualize_data(df)

def main():
    """Main function to execute the workflow."""
    # Step 1: Ingest Data
    df = load_data("insurance.csv")

    # Step 2: Process Data
    df = process_data(df)

    # Step 3: Store Data
    save_data(df, "processed_insurance.csv")

    # Step 4: Initial Analysis
    automated_analysis(df)

    # Step 5: Automate Tasks
    schedule.every().day.at("09:00").do(lambda: automated_analysis(df))

    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
