import os
import time
import requests
from zipfile import ZipFile
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException


BASE_URL = "https://cricsheet.org/downloads/" 
SAVE_PATH = "data" # Directory to hold the final processed data
RAW_ZIPS_DIR = os.path.join(SAVE_PATH, 'raw_json_zips') # Temporary folder for ZIPs
RAW_JSON_DIR = os.path.join(SAVE_PATH, 'raw_json')    # Folder for unzipped JSONs

# Dictionary starting the definition of the required match types.
MATCH_TYPES_TO_FIND = {
    "test": "Test matches",
    "odi": "One-day internationals",
    "t20": "T20 internationals", 
    "ipl": "Indian Premier League"
}

def setup_directories():
    #Create all necessary project folders.
    os.makedirs(RAW_ZIPS_DIR, exist_ok=True)
    os.makedirs(RAW_JSON_DIR, exist_ok=True)

def start_browser():
    #Initializes a headless Chrome WebDriver using Selenium Manager
    chrome_options = webdriver.ChromeOptions()
    # Run in headless mode to speed up the link discovery process
    chrome_options.add_argument("--headless=new") #without a visible graphical interface
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")#This argument tells Chrome to avoid using the /dev/shm directory, which is a shared memory file system
    
    # Selenium Manager automatically handles the driver
    try:
        driver = webdriver.Chrome(options=chrome_options)#Selenium Manager (built-in to modern Selenium) automatically finds and launches the correct ChromeDriver executable.
        return driver
    except Exception as e:
        print(f"Error initializing WebDriver: {e}")
        print("Please ensure Google Chrome is installed and updated.")
        return None

def get_zip_links(driver):
    #Uses Selenium to find the direct download URL for each JSON ZIP file
    driver.get(BASE_URL)
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.XPATH, "//table"))
    )

    zip_links = {}
    print("Extracting download links...")
    
    for short_name, match_type_text in MATCH_TYPES_TO_FIND.items():
        # ROBUST XPATH: Find the table cell containing the match type text, 
        # then find the 'JSON' link within its parent row (tr).
        json_link_xpath = (
            f"//td[contains(text(), '{match_type_text}')]/parent::tr//a[contains(text(), 'JSON')]"
        )
        
        try:
            link_element = driver.find_element(By.XPATH, json_link_xpath)
            zip_url = link_element.get_attribute('href')
            zip_links[short_name] = zip_url
            print(f"   Found {match_type_text} link.")
        except NoSuchElementException:
            print(f"   WARNING: Could not find link for {match_type_text}. Skipping.")
        
    return zip_links

def download_and_extract_zip(match_type, zip_url):
    #Downloads the ZIP using requests and extracts contents to the raw_json folder.
    
    # Check if files already exist (to avoid unnecessary downloads)
    final_folder = os.path.join(RAW_JSON_DIR, match_type)
    os.makedirs(final_folder, exist_ok=True)
    if any(f.endswith(".json") for f in os.listdir(final_folder)):
        print(f"Skipping {match_type} — JSONs already exist in {final_folder}.")
        return

    #  Download ZIP
    zip_filename = f"{match_type}_all_json.zip"
    zip_path = os.path.join(RAW_ZIPS_DIR, zip_filename)

    print(f"Downloading {zip_filename} for {match_type}...")
    r = requests.get(zip_url, stream=True)
    r.raise_for_status() # Raise exception for bad status codes
    
    with open(zip_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):#minimizing memory usage
            if chunk:
                f.write(chunk)
    print(f"Downloaded {zip_filename} successfully.")

    #  Extract JSONs
    print(f"Extracting {zip_filename} to {final_folder}...")
    with ZipFile(zip_path, "r") as zip_ref:
        # Extract to a dedicated sub-folder within raw_json
        zip_ref.extractall(final_folder) 
    
    #  Cleanup
    os.remove(zip_path)
    print(f"Extracted and removed {zip_filename}\n")


if __name__ == "__main__":
    setup_directories()
    
    browser = start_browser()
    if not browser:
        exit()

    try:
        # Use Selenium to find the direct links
        zip_links = get_zip_links(browser)
        print(f"Found direct links for {len(zip_links)} match types: {list(zip_links.keys())}\n")

        # Close the browser once links are found (Selenium is no longer needed)
        browser.quit()

        # Use requests to download and extract the data
        for match_type, zip_url in zip_links.items():
            try:
                download_and_extract_zip(match_type, zip_url)
            except Exception as e:
                print(f"ERROR processing {match_type}: {e}")
                
    except Exception as e:
        print(f"FATAL ERROR during scraping: {e}")
        if 'browser' in locals() and browser:
            browser.quit()

    print("Data Acquisition Phase Complete")
    print(f"Raw match data is organized in the '{RAW_JSON_DIR}' folder.")