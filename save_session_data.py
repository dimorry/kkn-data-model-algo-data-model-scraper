from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    # Launch Edge browser (headed mode)
    browser = p.chromium.launch(
        channel="msedge",  # 🚀 tells Playwright to use Microsoft Edge
        headless=False      # show the browser window
    )
    
    # Open a new page
    page = browser.new_page()
    
    # Navigate to a site
    page.goto("https://knowledge.kinaxis.com/s/")
    
    # Optional: pause for Playwright Inspector debugging
    page.pause()

    # todo: click on resume or hit F8 on the Playwright Inspector to continue
    context = page.context
    storage_state = context.storage_state(path="session_data.json")

    
    # Close the browser
    browser.close()
