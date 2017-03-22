I've created this small framework to help myself in scraping. So it's basically suits my needs and I don't know about yours.

It supports multiple threads and rotating proxies. It has filter class to run against the raw lists (for example from gatherproxy.com), filtering out buggy or timing out proxies so they won't slow the scrape.

You can even make it automatically refresh the lists from gatherproxy.com while scraping (as proxies degrade with time).