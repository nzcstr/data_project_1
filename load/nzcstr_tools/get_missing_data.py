import requests
from bs4 import BeautifulSoup
import urllib.parse

# Wikipedia REST
# #title = "Squid Game".split()
# title = "Dear White People".split()
# title = [w.capitalize() for w in title]
# title = "_".join(title)
#todo: Wikipedia limits requests to up to 200 request per day. Need to find a work around to implement this function
def get_director(title: str) -> str:
    title = title.split()
    title = [w.capitalize() for w in title]
    title = "_".join(title)
    director = None
    url = f"https://en.wikipedia.org/api/rest_v1/page/html/{title}"

    r = requests.get(url)
    data = BeautifulSoup(r.content, "html.parser")
    # Get the director which is (often) located in the info box as "Directed by"

    try:
        director = data.find("th", string="Directed by").find_next_sibling().text
    except:
        print(f"Could not find director for {title}")
    finally:
        return director

