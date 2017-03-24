I've created this small framework to help myself in scraping. So it basically suits my needs. I've tried to make it as flexible as possible—because the sites I need to scrape are very diverse. Retr stands for RETRieve and RETRy. It does both.

# Features
- multiple threads (legacy version) or async functions.
- Proxy rotation. It won't stop trying new proxies until the page is retrieved.
-- It rearranges the proxies in the list, putting bad ones in the end.
- Filter engine to run against the raw lists (for example from gatherproxy.com).

# Usage

## Validation of pages
As it keeps retrying the page through different proxies, we need to distinguish between the situation when the proxy returns an error, or the site in question returns a (legitimate) error. To this end, you can add a validation function. Default one checks for the http response code 200, otherwise retries. You can do anything here, for example check for some string you expect in the page (some proxies return admin pages for example).
