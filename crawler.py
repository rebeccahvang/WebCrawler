import logging
import re
from lxml import html
from urllib.parse import urlparse, urljoin, parse_qs
from bs4 import BeautifulSoup, Comment
from collections import defaultdict, Counter
import requests
from string import punctuation
import re

logger = logging.getLogger(__name__)

STOP_WORDS = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", "you'll", "you'd",
              'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's",
              'her', 'hers', 'herself', 'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves',
              'what', 'which', 'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are', 'was',
              'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an',
              'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with',
              'about',
              'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from',
              'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here',
              'there',
              'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some',
              'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will',
              'just',
              'don', "don't", 'should', "should've", 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren',
              "aren't", 'couldn', "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn', "hasn't",
              'haven',
              "haven't", 'isn', "isn't", 'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan',
              "shan't", 'shouldn', "shouldn't", 'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn',
              "wouldn't"]


class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    def __init__(self, frontier, corpus):
        self.frontier = frontier
        self.corpus = corpus
        self.domainCount = defaultdict(int)
        self.URLcount = defaultdict(int)
        self.pathCount = defaultdict(int)
        self.previous_link = ""

        # analytics 1: subdomains
        self.subdomainCount = defaultdict(int)

        # analytic 2: valid outlinks
        self.maxOutLinks = ["", 0]  # URL, number of out-links

        # analytic 3: downloaded urls and identified traps
        self.downloadedURLS = set()
        self.traps = set()

        # analytic 4: longest page
        self.longestPage = ["", 0]  # URL, number of words

        # analytic 5: 50 most common words
        self.commonWords = defaultdict(int)  # {(word: count),...}

    # ------ ANALYTICS 4 & 5 ------
    def count_words(self, content, url):
        """
        Count words from content of valid pages to find the 50 most common words.
        """
        # content = BeautifulSoup(url_data["content"], features="lxml")
        count = 0

        # divide text into token words and check against stopword
        temp = ""
        for c in content.get_text():
            if c.isalnum() and c.isascii():
                temp += c
            else:
                if len(temp) > 0:
                    if temp not in STOP_WORDS:
                        count += 1
                        self.commonWords[temp] += 1
                temp = ""

        if count > self.longestPage[1]:
            self.longestPage = [url, count]

    # ------ ANALYTICS 1 ------
    def subdomains(self):
        for url in self.downloadedURLS:
            parsed = urlparse(url)
            subdomain = parsed.netloc.split('.')
            for i in range(len(subdomain) - 2):
                if subdomain[i] != "www":
                    self.subdomainCount[subdomain[i]] += 1

    def analytics(self):
        analytic_file = open("analytics.txt", 'w')

        # analytic 1
        analytic_file.write('Subdomain and Counts: \n')
        for k, v in self.subdomainCount.items():
            analytic_file.write('{}: {}\n'.format(k, v))

        # analytic 2
        analytic_file.write('\nPage w/Most Valid Outlinks: {}\n'.format(self.maxOutLinks[0]))

        # analytic 3
        analytic_file.write("\nDownloaded URLS: \n")
        for url in self.downloadedURLS:
            analytic_file.write(url + '\n')

        analytic_file.write("\nTrap URLS: \n")
        for trap_url in self.traps:
            analytic_file.write(trap_url + '\n')

        # analytic 4
        analytic_file.write("\nURL with Longest Page: \n")
        analytic_file.write(self.longestPage[0])
        analytic_file.write("\n")

        # analytic 5
        sorted_words = sorted(self.commonWords.items(), key=lambda item: item[1], reverse=True)[:50]
        analytic_file.write("\n50 Most Common Words: \n")
        for w, c in sorted_words:
            analytic_file.write(str(w) + '\n')

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched,
                        len(self.frontier))
            url_data = self.corpus.fetch_url(url)

            count = 0
            for next_link in self.extract_next_links(url_data):
                if self.is_valid(next_link):
                    # ------ ANALYTICS 3 ------
                    self.downloadedURLS.add(next_link)
                    # ------ ANALYTICS 3 ------

                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)
                        count += 1
                else:
                    self.traps.add(next_link)
                # self.previous_link = next_link

            # ------ ANALYTICS 2 ------
            if count > self.maxOutLinks[1]:
                self.maxOutLinks = [url, count]
            # ------ ANALYTICS 2 ------

        self.subdomains()
        self.analytics()

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

        if url_data["is_redirected"] is True:
            url_data = self.corpus.fetch_url(url_data["final_url"])
        if url_data["content"] is not None and url_data["http_code"] != 404:
            content = BeautifulSoup(url_data["content"], features="lxml")
            self.count_words(content, url_data["url"])
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

            # Continuously repeating
            # count = sum(1 for a, b in zip(self.previous_link, url) if a != b) + abs(len(self.previous_link) - len(url))
            # if count < 3:
            #     return False

            query = parsed.query
            if len(query) > 0:
                # Visiting pages from same link/domain
                domainName = parsed.netloc + parsed.path
                self.domainCount[domainName] += 1
                if self.domainCount[domainName] > 10:
                    return False

                # Many query params or very long query (prevents dynamic URLs/calendar)
                query_count = parse_qs(query)
                if (len(query_count.values()) > 3):
                    return False

                for v in query_count.values():
                    if (len(v[0]) > 25):
                        return False

            path = parsed.path.split("/")
            # Extra directories
            if len(path) > 5:
                return False

            # Repeating paths in same URL
            visited_paths = defaultdict(int)
            for p in path:
                if visited_paths[p] > 2:
                    return False
                visited_paths[p] += 1

            # # Same path visited multiple times
            # if len(path) > 1:
            #     new_path = '/'.join(path[1:-1])
            #     self.pathCount[new_path] += 1
            #     if self.pathCount[new_path] > 5:
            #         return False

            return ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())

        except TypeError:
            print("TypeError for ", parsed)
            return False
