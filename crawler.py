import logging
import re
import lxml
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from collections import defaultdict, Counter

logger = logging.getLogger(__name__)

class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    def __init__(self, frontier, corpus):
        self.frontier = frontier
        self.corpus = corpus
        self.maxOutLinks = [] # URL, number of out-links
        self.longestPage = []  # URL, number of words
        self.commonWords = []  # [[word, count],[word, count]]

        # TODO:
        self.subdomainCount = defaultdict(int)
        self.downloadedURLS = []
        self.traps = []


    def count_words(self, url_data):
        """
        Count words from content of valid pages to find the 50 most common words.
        """
        content = url_data["content"]
        text_paragraph = (''.join(s.findAll(text=True)) for s in content.find_all('p'))
        count_paragraph = Counter((x.rstrip(punctuation).lower() for y in text_paragraph for x in y.split()))

        if count_paragraph > self.longestPage[1]:
            self.longestPage[1] = [url_data["url"], count_paragraph]

        self.commonWords.append(count_paragraph.most_common(50))
        self.commonWords = self.commonWords.sort()[:50]

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
            url_data = self.corpus.fetch_url(url)

            count = 0
            for next_link in self.extract_next_links(url_data):
                if self.is_valid(next_link):
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)
                        self.count_words(self.corpus.fetch_url(next_link))
                        count += 1
            if count > self.maxOutLinks[1]:
                self.maxOutLinks = [url, count]

    def extract_next_links(self, url_data):
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.

        Suggested library: lxml
        """
        outputLinks = []
        content = BeautifulSoup(url_data["content"], "lxml")
        anchorTags = content.find_all("a")

        url = url_data["url"]

        for link in anchorTags:
            href = link.attrs["href"]
            absoluteURL = urljoin(url, href)
            outputLinks.append(absoluteURL)

        return outputLinks

    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """
        parsed = urlparse(url)

        domain = parsed.netloc


        if parsed.scheme not in set(["http", "https"]):
            return False
        try:
            return ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())

        except TypeError:
            print("TypeError for ", parsed)
            return False

