import requests
from bs4 import BeautifulSoup
from pathlib import Path
from random import randint
from time import sleep
import csv

BASE_DIR = Path(__file__).resolve(strict=True).parent.parent


class Scraper:
    def __init__(self, url, pagination_index, user_agent=None):
        self.url = url
        self.pagination_index = pagination_index
        self.current_index = 0
        self.index_parameter = 'startIndex'
        self.page = None
        self.soup = None
        self.links = None
        self.linksToExplore = []
        self.next_url = None
        self.explore = True
        self.user_agent = user_agent
        self.csv = csv.writer(open(BASE_DIR / 'dataset/engelvoelkers_houses_bcn.csv', 'w'))
        self.csv.writerow(['id', 'title', 'location', 'location_status', 'status', 'year', 'area', 'bathrooms',
                           'bedrooms', 'heating_type', 'energy_class', 'price'])

    def __get_page(self, count=0):

        # Some pages don't work without headers. See robots.txt
        if not self.next_url:
            page_ = requests.get(self.url, headers={"User-Agent": self.user_agent})
        else:
            page_ = requests.get(self.next_url, headers={"User-Agent": self.user_agent})

        if not page_.ok:  # Only requests 2XX are valid
            if count >= 5:
                raise Exception(f"Could not get page {self.url}")
            sleep(randint(1, 5))
            self.__get_page(count+1)

        self.page = page_

    def __get_soup(self):
        soup_ = BeautifulSoup(self.page.content, features="html.parser")
        self.soup = soup_

    def __get_links(self):
        self.links = self.soup.find_all('a')

    def __filter_links(self):

        explorer_links = []
        for link in self.links:
            str_link = link.get('href')  # Sometimes it's None
            if str_link and '/propiedad/' in link.get('href'):
                explorer_links.append(link.get('href'))
        self.linksToExplore = self.linksToExplore + explorer_links

        if len(explorer_links) == 0:  # No more links to explore
            self.explore = False

    def __get_next_link(self):
        index = self.url.index(self.index_parameter)
        self.current_index += self.pagination_index

        if self.next_url is None:  # There are 2 advertisements on the first page instead of houses
            self.current_index -= 2

        self.next_url = self.url[:index + len(self.index_parameter) + 1] + str(self.current_index)
        self.next_url += self.url[index + len(self.index_parameter) + 2:]

    def __scrape_sub_pages(self):

        for link in self.linksToExplore:

            try:
                subpage_ = requests.get(link, headers={"User-Agent": self.user_agent})
                soup_ = BeautifulSoup(subpage_.content, features="html.parser")

                title = soup_.find('h1', class_='ev-exposee-title ev-exposee-headline').text
                location = soup_.find('div', class_='ev-exposee-content ev-exposee-subtitle').text
                fact_titles = [title.text for title in soup_.find_all('div', class_='ev-key-fact-title')]
                fact_values = [value.text for value in soup_.find_all('div', class_='ev-key-fact-value')]
                detail_facts = [detail.text for detail in soup_.find_all('li', class_='ev-exposee-detail-fact')]

                bedrooms, bathrooms, area, price = None, None, None, None
                for i in range(len(fact_titles)):
                    category = fact_titles[i].replace(" ", "")
                    if category == 'Dormitorios' or category == 'Cuartos':
                        bedrooms = fact_values[i]
                    elif category == 'Baños':
                        bathrooms = fact_values[i]
                    elif category == 'Superficiehabitableaprox.' or category == 'Superficieconstruidaaprox.':
                        area = fact_values[i]
                    elif category == 'Precio':
                        price = fact_values[i]

                try:
                    index = location.index("Barcelona")
                    location = location[index+11:]
                except ValueError:
                    location = None

                id_, year, status, location_status, energy_class, heating_type = None, None, None, None, None, None
                for detail in detail_facts:
                    if 'E&V ID' in detail:
                        id_ = detail[detail.index('E&V ID')+7:]
                    elif 'Año de construcción' in detail:
                        year = detail[detail.index('Año de construcción')+20:]
                    elif 'Estado' in detail:
                        status = detail[detail.index('Estado')+7:]
                    elif 'Ubicación' in detail:
                        location_status = detail[detail.index('Ubicación')+10:]
                    elif 'Clase de eficiencia energética' in detail:
                        energy_class = detail[detail.index('Clase de eficiencia energética')+31:]
                    elif 'Tipo de calefacción' in detail:
                        heating_type = detail[detail.index('Tipo de calefacción')+21:]

                # Now we have extracted the values we want
                self.__preprocess_and_safe(id_, title, location, bedrooms, bathrooms, area, price, year, status, location_status, energy_class, heating_type)

            except:
                pass

    def __preprocess_and_safe(self, id_, title, location, bedrooms, bathrooms, area, price, year, status, location_status,
                              energy_class, heating_type):

        # We remove the commas that can appear
        id_ = id_.replace(",", " ").replace(" ", "") if id_ is not None else None
        title = title[1:].replace(",", " ") if title is not None else None
        location = location.replace(",", " ") if location is not None else None
        location_status = location_status.replace(",", " ").replace(" ", "") if location_status is not None else None
        status = status.replace(",", " ").replace(" ", "") if status is not None else None
        year = year.replace(",", "").replace(" ", "") if year is not None else None
        area = area.replace(",", "").replace(" ", "") if area is not None else None
        bathrooms = bathrooms.replace(",", "").replace(" ", "") if bathrooms is not None else None
        bedrooms = bedrooms.replace(",", "").replace(" ", "") if bedrooms is not None else None
        heating_type = heating_type.replace(",", " ").replace(" ", "") if heating_type is not None else None
        energy_class = energy_class.replace(",", "").replace(" ", "") if energy_class is not None else None
        price = price.replace(",", "").replace(" ", "") if price is not None else None

        # And safe it in the csv
        self.csv.writerow([id_, title, location, location_status, status, year, area, bathrooms, bedrooms, heating_type,
                           energy_class, price])

    def scrape(self):

        # First we search for all the links we want to explore
        while self.explore:
            self.__get_page()
            self.__get_soup()
            self.__get_links()
            self.__filter_links()
            self.__get_next_link()

        # Now we scrape those links, search the relevant information and save it in a CSV file
        self.__scrape_sub_pages()


if __name__ == '__main__':

    # This script scrapes 1250 links and takes about 10 minutes
    # The page is from engel&volkers a real state company. And the information scraped is related to house selling.
    user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/5\
    37.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36"
    scrapper = Scraper("https://www.engelvoelkers.com/es/search/?q=&startIndex=0&businessArea=residential&sortOrder=DESC&sortField=newestProfileCreationTimestamp&pageSize=18&facets=bsnssr%3Aresidential%3Bcntry%3Aspain%3Bobjcttyp%3Acondo%3Brgn%3Abarcelona%3Btyp%3Abuy%3B", 18, user_agent)
    scrapper.scrape()
