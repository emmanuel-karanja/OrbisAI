# browser_utils.py
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

def setup_browser(logger):
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    try:
        service = Service(ChromeDriverManager().install())
        browser = webdriver.Chrome(service=service, options=options)
        return browser
    except WebDriverException as e:
        logger.error(f"❌ Failed to launch browser: {e}")
    except Exception as e:
        logger.error(f"❌ Unexpected browser error: {e}")
    return None
