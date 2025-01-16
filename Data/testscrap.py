from time import sleep
import requests
import pandas as pd
from bs4 import BeautifulSoup

def extract_data(soup):
    users = []
    userReviewNum = []
    locations = []
    dates = []
    reviews = []
    ratings = []
    
    # Extract all reviews on the page
    review_blocks = soup.find_all('article', {'class': 'paper_paper__1PY90 paper_outline__lwsUX card_card__lQWDv card_noPadding__D8PcU styles_reviewCard__hcAvl '})
    for block in review_blocks:
        try:
            # Extract individual fields
            user = block.find('span', {'class': 'typography_heading-xxs__QKBS8 typography_appearance-default__AAY17'}).get_text()
            location = block.find('div', {'class': 'typography_body-m__xgxZ_ typography_appearance-subtle__8_H2l styles_detailsIcon__Fo_ua'}).get_text() if block.find('div', {'class': 'typography_body-m__xgxZ_ typography_appearance-subtle__8_H2l styles_detailsIcon__Fo_ua'}) else None
            review_num = block.find('span', {'class': 'typography_body-m__xgxZ_ typography_appearance-subtle__8_H2l'}).get_text()
            date = block.find('div', {'class': 'styles_reviewHeader__iU9Px'}).get_text()
            review_content = block.find('div', {'class': 'styles_reviewContent__0Q2Tg'}).get_text()
            rating = block.find('div', {'class': 'styles_reviewHeader__iU9Px'})['data-service-review-rating']

            # Append extracted data only if all fields are present
            if user and review_content and rating:
                users.append(user)
                userReviewNum.append(review_num)
                locations.append(location)
                dates.append(date)
                reviews.append(review_content)
                ratings.append(rating)
        except AttributeError:
            print("Skipped a review due to missing fields.")

    return users, userReviewNum, locations, dates, reviews, ratings


from_page = 1
to_page = 15
company = 'www.pureformulas.com'

# Initialize empty lists for final data
final_users, final_review_nums, final_locations, final_dates, final_reviews, final_ratings = ([] for _ in range(6))

for i in range(from_page, to_page + 1):
    print(f"Scraping page {i}...")
    result = requests.get(f"https://www.trustpilot.com/review/{company}?page={i}")
    soup = BeautifulSoup(result.content, features='html.parser')

    # Extract data for the page
    users, review_nums, locations, dates, reviews, ratings = extract_data(soup)

    # Append page data to final lists
    final_users.extend(users)
    final_review_nums.extend(review_nums)
    final_locations.extend(locations)
    final_dates.extend(dates)
    final_reviews.extend(reviews)
    final_ratings.extend(ratings)

    # To avoid throttling
    sleep(1)

print({key: len(value) for key, value in {
    "Users": final_users,
    "Total reviews": final_review_nums,
    "Locations": final_locations,
    "Dates": final_dates,
    "Content": final_reviews,
    "Ratings": final_ratings
}.items()})

review_data = pd.DataFrame({
    'Username': final_users,
    'Total reviews': final_review_nums,
    'Location': final_locations,
    'Date': final_dates,
    'Content': final_reviews,
    'Rating': final_ratings
})

csv_file = 'scraped_reviews.csv'
review_data.to_csv(csv_file, index=False, encoding='utf-8')
print(f"Data saved to {csv_file}")
