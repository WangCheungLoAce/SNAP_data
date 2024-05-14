import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

options = Options()
options.headless = False  # Set to True if you want to run in headless mode (without GUI)
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

try:
    # Navigate to the SNAP retailer locator page
    url = 'https://www.fns.usda.gov/snap/retailer-locator'
    driver.get(url)
    
    time.sleep(3)
    
    # Click on the historical data link
    his_data_link = driver.find_element(By.XPATH, '//a[@href="/snap/retailer/historicaldata" and @data-once="extmodal"]')
    his_data_link.click()
    
    time.sleep(5)
    
    # Click on the download data link
    # Corrected XPath for download link
    download_data = driver.find_element(By.XPATH, '//a[contains(@href, ".zip") and @data-once="extmodal"]')
    download_data.click()
    
    time.sleep(20)
    
    # Get the updated date from the webpage
    updated_date_element = driver.find_element(By.CLASS_NAME, 'created-date')
    updated_date = updated_date_element.text
    
    print(f"Dataset successfully downloaded & {updated_date}")
    
except NoSuchElementException as e:
    print(f"Error: Element not found - {str(e)}")

finally:
    driver.quit()