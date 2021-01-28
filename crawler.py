import logging
import re
from lxml import html
from urllib.parse import urlparse, urljoin, parse_qs
from bs4 import BeautifulSoup
from collections import defaultdict, Counter
import requests
from string import punctuation

logger = logging.getLogger(__name__)

class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    def __init__(self, frontier, corpus):
        self.frontier = frontier
        self.corpus = corpus
        self.domainCount = defaultdict(int)
        
        # analytics 1: subdomains
        # TODO:
        self.subdomainCount = defaultdict(int)

        # analytic 2: valid outlinks
        self.maxOutLinks = ["", 0] # URL, number of out-links

        # analytic 3: downloaded urls and identified traps
        # TODO:
        self.downloadedURLS = []
        self.traps = []

        # analytic 4: longest page
        self.longestPage = ["", 0]  # URL, number of words
        

        # analytic 5: 50 most common words
        self.commonWords = []  # [[word, count],[word, count]]

    # ------ ANALYTICS 4 & 5 ------
    # def count_words(self, url_data):
    #     """
    #     Count words from content of valid pages to find the 50 most common words.
    #     """
    #     r = requests.get(url_data["url"])
    #     content = BeautifulSoup(r.content)
    #
    #     text_paragraph = (''.join(s.findAll(text=True)) for s in content.findAll('p'))
    #     count_paragraph = Counter((x.rstrip(punctuation).lower() for y in text_paragraph for x in y.split()))
    #
    #     if len(count_paragraph) > self.longestPage[1]:
    #         self.longestPage[1] = [url_data["url"], count_paragraph]
    #
    #     self.commonWords.append(count_paragraph.most_common(50))
    #     self.commonWords = self.commonWords.sort()[:50]

    def analytics(self):
        analytic_file = open("analytics.txt", 'w')
        
        # analytic 1
        for k, v in self.subdomainCount.items():
            analytic_file.write('{}, {}\n'.format(k, v))
                
        # analytic 2
        analytic_file.write('Page w/Most Valid Outlinks: {}\n'.format(self.maxOutLinks[0]))

        # analytic 3
        analytic_file.write("Downloaded URLS: \n")
        for url in self.downloadedURLS:
            analytic_file.write(url + '\n')

        analytic_file.write("Trap URLS: \n")
        for trap_url in self.traps:
            analytic_file.write(trap_url + '\n')

        # analytic 4
        analytic_file.write("URL with Longest Page: \n")
        analytic_file.write(self.longestPage[0])
        analytic_file.write("\n")

        # analytic 5
        self.commonWords.sort(key = lambda x:x[1], reverse=True)
        analytic_file.write("50 Most Common Words | Word Occurrences: \n")
        analytic_file.write(self.commonWords)

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
                    # ------ ANALYTICS 1 ------
                    parsed = urlparse(url)
                    subdomain = parsed.netloc.split('.')[0]
                    self.subdomainCount[subdomain] += 1
                    # ------ ANALYTICS 1 ------

                    # ------ ANALYTICS 3 ------
                    self.downloadedURLS.append(next_link)
                    # ------ ANALYTICS 3 ------

                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)
                        # ------ ANALYTICS 4 & 5 ------
                        # self.count_words(url_data)
                        # ------ ANALYTICS 4 & 5 ------
                        count += 1
                else:
                    self.traps.append(next_link)
            
            # ------ ANALYTICS 2 ------
            if count > self.maxOutLinks[1]:
                self.maxOutLinks = [url, count]
            # ------ ANALYTICS 2 ------

        # self.analytics()

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
        for a in content.find_all('a', href=True):
            outputLinks.append(urljoin(url_data["url"], a['href']))

        return outputLinks

    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """

        parsed = urlparse(url)

        if parsed.scheme not in set(["http", "https"]):
            return False
        try:
            # Very long URL
            if len(url) > 100:
                return False
                
            # Visiting pages from same link/domain
            domainName = parsed.netloc + parsed.path
            self.domainCount[domainName] += 1
            if self.domainCount[domainName] > 10:
                return False
            
            # Many query params or very long query
            query = parsed.query
            if len(query) > 0:
                query_count = parse_qs(query)
                if(len(query_count.values()) > 3):
                    return False

                for v in query_count.values():
                    for inner_v in v:
                        if(len(inner_v) > 25):
                            return False

            path = parsed.path.split("/")
            # Extra directories
            if len(path) >= 5:
                return False

            # Repeating paths in same URL
            visited_paths = set()
            for p in path:
                if p in visited_paths:
                    return False
                elif p not in visited_paths:
                    visited_paths.add(p)

            return ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())

        except TypeError:
            print("TypeError for ", parsed)
            return False