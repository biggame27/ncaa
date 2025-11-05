"""Selenium utility functions for the NCAA scraper."""

import time
import logging
import os
import shutil
import random
from typing import List, Optional
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, WebDriverException, SessionNotCreatedException
from webdriver_manager.chrome import ChromeDriverManager

logger = logging.getLogger(__name__)


class SeleniumUtils:
    """Utility class for Selenium operations."""
    
    @staticmethod
    def create_driver(headless: bool = False, max_retries: int = 3) -> webdriver.Chrome:
        """
        Create and configure Chrome WebDriver with retry logic.
        
        Args:
            headless: Whether to run in headless mode
            max_retries: Maximum number of retry attempts
        
        Returns:
            Configured Chrome WebDriver
            
        Raises:
            SessionNotCreatedException: If unable to create driver after all retries
        """
        for attempt in range(max_retries):
            try:
                logger.info(f"Creating Chrome driver (attempt {attempt + 1}/{max_retries})")
                
                # Configure Chrome options
                options = Options()
                
                # Always run headless in Docker or when requested
                if headless or os.getenv('DOCKER_CONTAINER', 'false').lower() == 'true':
                    options.add_argument("--headless=new")
                
                # Essential Chrome options for stability and anti-detection
                options.add_argument("--no-sandbox")
                options.add_argument("--disable-dev-shm-usage")
                options.add_argument("--disable-gpu")
                options.add_argument("--disable-software-rasterizer")
                options.add_argument("--window-size=1920,1080")
                options.add_argument("--disable-setuid-sandbox")
                options.add_argument("--disable-background-timer-throttling")
                options.add_argument("--disable-backgrounding-occluded-windows")
                options.add_argument("--disable-renderer-backgrounding")
                options.add_argument("--disable-extensions")
                options.add_argument("--disable-plugins")
                options.add_argument("--disable-web-security")
                options.add_argument("--disable-features=VizDisplayCompositor")
                options.add_argument("--ignore-certificate-errors")
                options.add_argument("--ignore-ssl-errors")
                options.add_argument("--allow-running-insecure-content")
                options.add_argument("--remote-debugging-port=9222")
                options.add_argument("--disable-blink-features=AutomationControlled")
                options.add_argument("--disable-logging")
                options.add_argument("--disable-default-apps")
                options.add_argument("--disable-sync")
                options.add_argument("--disable-translate")
                options.add_argument("--hide-scrollbars")
                options.add_argument("--mute-audio")
                options.add_argument("--no-first-run")
                options.add_argument("--safebrowsing-disable-auto-update")
                options.add_argument("--disable-client-side-phishing-detection")
                options.add_argument("--disable-component-update")
                options.add_argument("--disable-domain-reliability")
                options.add_argument("--disable-features=TranslateUI")
                options.add_argument("--disable-ipc-flooding-protection")
                
                # Memory and performance optimizations to prevent hangs
                options.add_argument("--memory-pressure-off")
                options.add_argument("--max_old_space_size=4096")  # Limit memory usage
                options.add_argument("--disable-background-networking")
                options.add_argument("--disable-background-timer-throttling")
                options.add_argument("--disable-breakpad")
                options.add_argument("--disable-hang-monitor")
                options.add_argument("--disable-prompt-on-repost")
                options.add_argument("--disable-renderer-backgrounding")
                options.add_argument("--force-color-profile=srgb")
                options.add_argument("--metrics-recording-only")
                options.add_argument("--enable-automation")
                
                # Anti-detection measures
                options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
                options.add_argument("--accept-language=en-US,en;q=0.9")
                options.add_argument("--accept-encoding=gzip, deflate, br")
                options.add_argument("--accept=text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8")
                options.add_argument("--upgrade-insecure-requests=1")
                options.add_argument("--sec-fetch-site=none")
                options.add_argument("--sec-fetch-mode=navigate")
                options.add_argument("--sec-fetch-user=?1")
                options.add_argument("--sec-fetch-dest=document")
                options.add_argument("--cache-control=max-age=0")
                
                # Disable automation detection
                options.add_experimental_option("useAutomationExtension", False)
                options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
                options.add_experimental_option('useAutomationExtension', False)
                
                # Additional anti-detection measures
                prefs = {
                    "profile.default_content_setting_values": {
                        "notifications": 2,
                        "geolocation": 2,
                        "media_stream": 2,
                    },
                    "profile.managed_default_content_settings": {
                        "images": 1
                    }
                }
                options.add_experimental_option("prefs", prefs)
                
                # Set up WebDriverManager
                cache_dir = "/tmp/chrome-session" if os.path.exists("/tmp") else os.path.join(os.getcwd(), "chrome-session")
                os.makedirs(cache_dir, exist_ok=True)
                
                # Set environment variables for WebDriverManager
                os.environ['WDM_LOCAL'] = '1'
                os.environ['WDM_CACHE_DIR'] = cache_dir
                os.environ['WDM_LOG_LEVEL'] = '0'  # Disable WebDriverManager logging
                
                # Try to get Chrome binary path
                chrome_binary = os.getenv('CHROME_BIN', '/usr/bin/google-chrome')
                if os.path.exists(chrome_binary):
                    options.binary_location = chrome_binary
                
                # Create service with WebDriverManager
                try:
                    # Try with regular Chrome first
                    service = Service(ChromeDriverManager(cache_valid_range=1).install())
                except Exception as e:
                    logger.warning(f"Failed to create ChromeDriver service: {e}")
                    # Fallback: try without cache validation
                    service = Service(ChromeDriverManager().install())
                
                # Create driver
                driver = webdriver.Chrome(service=service, options=options)
                
                # Set timeouts to prevent infinite hangs
                driver.set_page_load_timeout(60)  # Max 60 seconds for page load
                driver.set_script_timeout(30)     # Max 30 seconds for scripts
                
                # Execute anti-detection scripts
                # Note: These are fast operations right after driver creation, so timeout protection
                # is less critical here, but we wrap in try/except for safety
                try:
                    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                    driver.execute_script("Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]})")
                    driver.execute_script("Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']})")
                    driver.execute_script("window.chrome = {runtime: {}}")
                    driver.execute_script("Object.defineProperty(navigator, 'permissions', {get: () => ({query: () => Promise.resolve({state: 'granted'})})})")
                except Exception as e:
                    logger.warning(f"Error setting anti-detection properties: {e}")
                    # Continue anyway - these are nice-to-have
                
                # Test the driver
                try:
                    driver.get("about:blank")
                    driver.execute_script("return navigator.userAgent")
                except Exception as e:
                    logger.warning(f"Driver test failed: {e}")
                    # Continue anyway - driver might still work
                
                logger.info("Chrome driver created successfully")
                return driver
                
            except SessionNotCreatedException as e:
                logger.warning(f"Session creation failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    # Clean up any partial driver instances
                    SeleniumUtils._cleanup_driver_resources()
                    time.sleep(2)  # Wait before retry
                    continue
                else:
                    logger.error(f"Failed to create Chrome driver after {max_retries} attempts")
                    raise
            except Exception as e:
                logger.warning(f"Unexpected error creating driver (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    SeleniumUtils._cleanup_driver_resources()
                    time.sleep(2)
                    continue
                else:
                    logger.error(f"Failed to create Chrome driver after {max_retries} attempts")
                    raise SessionNotCreatedException(f"Failed to create Chrome driver: {e}")
    
    @staticmethod
    def _cleanup_driver_resources():
        """Clean up driver-related resources and processes."""
        try:
            # Kill any existing Chrome processes
            if os.name == 'nt':  # Windows
                os.system('taskkill /f /im chrome.exe 2>nul')
                os.system('taskkill /f /im chromedriver.exe 2>nul')
            else:  # Unix-like systems
                os.system('pkill -f chrome 2>/dev/null')
                os.system('pkill -f chromedriver 2>/dev/null')
            
            # Clean up temporary directories
            temp_dirs = ["/tmp/chrome-session", "chrome-session"]
            for temp_dir in temp_dirs:
                if os.path.exists(temp_dir):
                    try:
                        shutil.rmtree(temp_dir, ignore_errors=True)
                    except Exception:
                        pass
                        
        except Exception as e:
            logger.warning(f"Error during cleanup: {e}")
    
    @staticmethod
    def human_like_delay(min_seconds: float = 1.0, max_seconds: float = 3.0):
        """Add a human-like random delay."""
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)
    
    @staticmethod
    def safe_driver_operation(driver, operation, timeout=10, default_return=None, operation_name="driver operation"):
        """
        Safely execute a driver operation with timeout protection.
        
        This prevents read timeout errors by wrapping driver operations in a thread
        with a timeout, so if ChromeDriver becomes unresponsive, we don't wait
        indefinitely.
        
        Args:
            driver: WebDriver instance
            operation: Callable that performs the driver operation
            timeout: Maximum time to wait in seconds
            default_return: Value to return if operation times out
            operation_name: Name of operation for logging
        
        Returns:
            Result of operation or default_return if timeout
        
        Raises:
            Exception: If operation raises an exception (not timeout)
        """
        import threading
        result = [None]
        exception = [None]
        completed = [False]
        
        def execute():
            try:
                result[0] = operation()
                completed[0] = True
            except Exception as e:
                exception[0] = e
                completed[0] = True
        
        thread = threading.Thread(target=execute)
        thread.daemon = True
        thread.start()
        thread.join(timeout=timeout)
        
        if not completed[0]:
            logger.warning(f"Driver operation '{operation_name}' timed out after {timeout} seconds")
            return default_return
        elif exception[0]:
            raise exception[0]
        else:
            return result[0]
    
    @staticmethod
    def safe_quit_driver(driver: Optional[webdriver.Chrome]) -> bool:
        """
        Safely quit a WebDriver instance with timeout protection.
        
        This prevents read timeout errors during cleanup by using timeout
        protection on all driver operations.
        
        Args:
            driver: WebDriver instance to quit
        
        Returns:
            True if quit successfully, False otherwise
        """
        if not driver:
            return True
            
        try:
            # Try to get window handles with timeout protection
            handles = SeleniumUtils.safe_driver_operation(
                driver,
                lambda: driver.window_handles,
                timeout=5,
                default_return=[],
                operation_name="get window handles"
            )
            
            # Close windows with timeout protection
            if handles:
                for handle in handles:
                    try:
                        SeleniumUtils.safe_driver_operation(
                            driver,
                            lambda h=handle: driver.switch_to.window(h),
                            timeout=2,
                            operation_name=f"switch to window {handle}"
                        )
                        SeleniumUtils.safe_driver_operation(
                            driver,
                            lambda: driver.close(),
                            timeout=2,
                            operation_name="close window"
                        )
                    except Exception as e:
                        logger.debug(f"Error closing window {handle}: {e}")
                        pass  # Continue with other windows
            
            # Quit driver with timeout protection
            SeleniumUtils.safe_driver_operation(
                driver,
                lambda: driver.quit(),
                timeout=10,
                operation_name="quit driver"
            )
            logger.info("Driver quit successfully")
            return True
            
        except Exception as e:
            logger.warning(f"Error quitting driver: {e}")
            # Try one more time without timeout protection as last resort
            try:
                driver.quit()
                return True
            except Exception:
                return False
    
    @staticmethod
    def wait_for_element(driver: webdriver.Chrome, by: By, value: str, timeout: int = 15) -> Optional[object]:
        """
        Wait for an element to be present on the page.
        
        Args:
            driver: WebDriver instance
            by: Selenium By selector
            value: Selector value
            timeout: Maximum time to wait in seconds
        
        Returns:
            WebElement if found, None otherwise
        """
        try:
            wait = WebDriverWait(driver, timeout)
            element = wait.until(EC.presence_of_element_located((by, value)))
            return element
        except TimeoutException:
            logger.warning(f"Element not found: {by}={value}")
            return None
    
    @staticmethod
    def wait_for_elements(driver: webdriver.Chrome, by: By, value: str, timeout: int = 15) -> List[object]:
        """
        Wait for elements to be present on the page.
        
        Args:
            driver: WebDriver instance
            by: Selenium By selector
            value: Selector value
            timeout: Maximum time to wait in seconds
        
        Returns:
            List of WebElements found
        """
        try:
            wait = WebDriverWait(driver, timeout)
            elements = wait.until(EC.presence_of_all_elements_located((by, value)))
            return elements
        except TimeoutException:
            logger.warning(f"Elements not found: {by}={value}")
            return []
    
    @staticmethod
    def safe_click(element) -> bool:
        """
        Safely click an element.
        
        Args:
            element: WebElement to click
        
        Returns:
            True if click successful, False otherwise
        """
        try:
            element.click()
            return True
        except WebDriverException as e:
            logger.warning(f"Failed to click element: {e}")
            return False
    
    @staticmethod
    def safe_get_text(element) -> str:
        """
        Safely get text from an element.
        
        Args:
            element: WebElement to get text from
        
        Returns:
            Element text or empty string if failed
        """
        try:
            return element.text.strip()
        except WebDriverException as e:
            logger.warning(f"Failed to get text from element: {e}")
            return ""
    
    @staticmethod
    def safe_get_attribute(element, attribute: str) -> str:
        """
        Safely get attribute from an element.
        
        Args:
            element: WebElement to get attribute from
            attribute: Attribute name
        
        Returns:
            Attribute value or empty string if failed
        """
        try:
            return element.get_attribute(attribute) or ""
        except WebDriverException as e:
            logger.warning(f"Failed to get attribute {attribute} from element: {e}")
            return ""
    
    @staticmethod
    def check_page_loaded(driver: webdriver.Chrome, expected_elements: List[str], timeout: int = 15) -> bool:
        """
        Check if page loaded successfully by looking for expected elements.
        
        Args:
            driver: WebDriver instance
            expected_elements: List of CSS selectors to look for
            timeout: Maximum time to wait in seconds
        
        Returns:
            True if page loaded successfully, False otherwise
        """
        for selector in expected_elements:
            try:
                wait = WebDriverWait(driver, timeout)
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                return True
            except TimeoutException:
                continue
        
        return False
    
    @staticmethod
    def check_for_errors(driver: webdriver.Chrome) -> Optional[str]:
        """
        Check for common error pages.
        
        Args:
            driver: WebDriver instance
        
        Returns:
            Error message if error found, None otherwise
        """
        try:
            # Check for 404 error
            if "404" in driver.title or "not found" in driver.title.lower():
                return "Page not found (404)"
            
            # Check for error page
            if "error" in driver.title.lower():
                return "Error page detected"
            
            # Check for specific NCAA 404 error
            error_404 = driver.find_elements(By.CLASS_NAME, "error-404")
            if error_404:
                return "NCAA 404 error - 'That's a foul on us...'"
            
            # Check for unavailable content
            if "unavailable" in driver.page_source.lower():
                return "Content unavailable"
            
            # Check for 500 server error
            if "500" in driver.title or "internal server error" in driver.title.lower():
                return "Internal server error (500)"
            
            # Check for 403 forbidden error
            if "403" in driver.title or "forbidden" in driver.title.lower():
                return "Access forbidden (403)"
            
            # Check for 401 unauthorized error
            if "401" in driver.title or "unauthorized" in driver.title.lower():
                return "Unauthorized access (401)"
            
            # Check for timeout errors
            if "timeout" in driver.title.lower() or "time out" in driver.title.lower():
                return "Page timeout error"
            
            # Check for maintenance mode
            if "maintenance" in driver.page_source.lower() or "under maintenance" in driver.page_source.lower():
                return "Site under maintenance"
            
            # Check for rate limiting
            if "rate limit" in driver.page_source.lower() or "too many requests" in driver.page_source.lower():
                return "Rate limit exceeded"
            
            # Check for network errors
            if "network error" in driver.page_source.lower() or "connection error" in driver.page_source.lower():
                return "Network connection error"
            
            # Check for JavaScript errors in console
            try:
                logs = driver.get_log('browser')
                for log in logs:
                    if log['level'] == 'SEVERE' and any(keyword in log['message'].lower() for keyword in ['error', 'failed', 'exception']):
                        return f"JavaScript error detected: {log['message'][:100]}"
            except Exception:
                pass  # Ignore if we can't get console logs
            
            return None
            
        except Exception as e:
            logger.warning(f"Error checking for page errors: {e}")
            return None
    
    @staticmethod
    def check_http_status(driver: webdriver.Chrome) -> Optional[str]:
        """
        Check HTTP status code and network errors.
        
        Args:
            driver: WebDriver instance
        
        Returns:
            Error message if HTTP error found, None otherwise
        """
        try:
            # Check if we can get the current URL and status
            current_url = driver.current_url
            
            # Check for common error URLs
            if "error" in current_url.lower() or "404" in current_url.lower():
                return f"Error URL detected: {current_url}"
            
            # Try to get HTTP status from performance logs
            try:
                logs = driver.get_log('performance')
                for log in logs:
                    message = log.get('message', '')
                    if 'Network.responseReceived' in message:
                        # Parse the response data
                        import json
                        response_data = json.loads(message)
                        if 'response' in response_data.get('message', {}):
                            status = response_data['message']['response'].get('status', 200)
                            if status >= 400:
                                return f"HTTP {status} error detected"
            except Exception:
                pass  # Ignore if we can't get performance logs
            
            return None
            
        except Exception as e:
            logger.warning(f"Error checking HTTP status: {e}")
            return None
