from apify_client import ApifyClient
import json
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field, field_validator
from datetime import datetime


# Pydantic Models
class Review(BaseModel):
    """Model for a single Google Maps review."""
    place_id: str = Field(..., alias="placeId", description="Google Maps place ID")
    review_id: Optional[str] = Field(None, alias="reviewId", description="Unique review ID")
    text: Optional[str] = Field(None, description="Review text content")
    rating: Optional[float] = Field(None, ge=1, le=5, description="Review rating (1-5 stars)")
    author_name: Optional[str] = Field(None, alias="name", description="Review author name")
    author_url: Optional[str] = Field(None, alias="reviewUrl", description="URL to the review")
    published_at: Optional[str] = Field(None, alias="publishedAtDate", description="Publication date")
    likes_count: Optional[int] = Field(None, alias="likesCount", description="Number of likes")
    review_image_urls: Optional[List[str]] = Field(None, alias="reviewImageUrls", description="URLs of review images")
    response_text: Optional[str] = Field(None, alias="responseText", description="Owner's response to review")
    response_date: Optional[str] = Field(None, alias="responseDate", description="Response date")

    class Config:
        populate_by_name = True
        extra = "allow"


class ScraperConfig(BaseModel):
    """Configuration for the Google Maps review scraper."""
    api_token: str = Field(..., description="Apify API token", min_length=1)
    place_ids: List[str] = Field(..., description="List of Google Maps place IDs", min_length=1)
    max_reviews: int = Field(100, ge=1, le=1000, description="Maximum reviews per place")
    language: str = Field("en", description="Language code for reviews")

    @field_validator('place_ids')
    @classmethod
    def validate_place_ids(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError("At least one place_id must be provided")
        for place_id in v:
            if not place_id.strip():
                raise ValueError("Place IDs cannot be empty strings")
        return v


class ReviewsResponse(BaseModel):
    """Response model containing reviews organized by place ID."""
    reviews_by_place: Dict[str, List[Review]] = Field(..., description="Reviews organized by place ID")
    total_reviews: int = Field(..., description="Total number of reviews fetched")
    total_places: int = Field(..., description="Total number of places")

    @classmethod
    def from_raw_data(cls, raw_data: Dict[str, List[Dict[str, Any]]]) -> "ReviewsResponse":
        """Create ReviewsResponse from raw dictionary data."""
        reviews_by_place = {}
        total_reviews = 0

        for place_id, reviews in raw_data.items():
            parsed_reviews = []
            for review_data in reviews:
                try:
                    # Handle different possible field names
                    if "text" not in review_data and "reviewText" in review_data:
                        review_data["text"] = review_data["reviewText"]

                    parsed_reviews.append(Review(**review_data))
                except Exception as e:
                    print(f"Warning: Failed to parse review: {e}")
                    continue

            reviews_by_place[place_id] = parsed_reviews
            total_reviews += len(parsed_reviews)

        return cls(
            reviews_by_place=reviews_by_place,
            total_reviews=total_reviews,
            total_places=len(reviews_by_place)
        )


class ReviewTextsResponse(BaseModel):
    """Response model containing only review texts organized by place ID."""
    texts_by_place: Dict[str, List[str]] = Field(..., description="Review texts organized by place ID")
    total_reviews: int = Field(..., description="Total number of reviews")
    total_places: int = Field(..., description="Total number of places")


def get_google_maps_reviews(
    config: ScraperConfig
) -> ReviewsResponse:
    """
    Fetch Google Maps reviews for a list of place IDs.

    Args:
        config: ScraperConfig object with API token and scraping parameters

    Returns:
        ReviewsResponse object containing parsed reviews organized by place ID
    """
    # Initialize the ApifyClient with your API token
    client = ApifyClient(config.api_token)

    # Prepare the Actor input
    run_input = {
        "placeIds": config.place_ids,
        "maxReviews": config.max_reviews,
        "language": config.language,
    }

    # Run the Actor and wait for it to finish
    print(f"Running scraper for {len(config.place_ids)} place(s)...")
    run = client.actor("compass/google-maps-reviews-scraper").call(run_input=run_input)

    # Fetch Actor results from the run's dataset
    print(f"Check your data here: https://console.apify.com/storage/datasets/{run['defaultDatasetId']}")

    # Organize results by place ID
    results: Dict[str, List[Dict[str, Any]]] = {place_id: [] for place_id in config.place_ids}

    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        place_id = item.get("placeId")
        if place_id in results:
            results[place_id].append(item)

    # Convert to Pydantic response
    return ReviewsResponse.from_raw_data(results)


def get_google_maps_review_texts(
    config: ScraperConfig
) -> ReviewTextsResponse:
    """
    Fetch Google Maps review texts for a list of place IDs.

    Args:
        config: ScraperConfig object with API token and scraping parameters

    Returns:
        ReviewTextsResponse object containing review text strings organized by place ID
    """
    # Get full review data
    reviews_response = get_google_maps_reviews(config)

    # Extract just the text from each review
    texts_by_place: Dict[str, List[str]] = {}
    total_reviews = 0

    for place_id, reviews in reviews_response.reviews_by_place.items():
        texts = [review.text for review in reviews if review.text]
        texts_by_place[place_id] = texts
        total_reviews += len(texts)

    return ReviewTextsResponse(
        texts_by_place=texts_by_place,
        total_reviews=total_reviews,
        total_places=len(texts_by_place)
    )


def save_reviews_to_json(
    response: ReviewsResponse | ReviewTextsResponse,
    output_file: str = "reviews.json"
) -> None:
    """
    Save reviews response to a JSON file.

    Args:
        response: ReviewsResponse or ReviewTextsResponse object
        output_file: Path to output JSON file (default: "reviews.json")
    """
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(response.model_dump_json(indent=2, by_alias=True))
    print(f"Reviews saved to {output_file}")


def main():
    api_token = ""
    place_ids = [
        "ChIJl8KQ5PhtdEgR2YaqLVwelc0",
        "ChIJT7ed6p5xdEgRulLzeHDBATU",
        "ChIJRVNTypBxdEgRxrOEG7oKw0A",
        "ChIJn5y1bx0PAE0RcCWdYFsSXwM",
        "ChIJ4ZM1WJdxdEgR8qWF5pdM9ck",
        "ChIJwXTzRxdxdEgR4z_tSKrWovY",
        "ChIJ70XU0G5xdEgRKT7fbDhtrkA",
        "ChIJqYIxOHdxdEgRdOXYazAwpLc",
        "ChIJ_znqbPtxdEgRcDxTopdNZRU",
        "ChIJX9HvFl9xdEgRRE-Ap9uyzY0",
        "ChIJi_EN28V1dEgRumM8pGWTh7w",
        "ChIJnSYmE_h1dEgR-TT1kBgD3Ms",
        "ChIJ8TELSxx3x0IRHy0SaE18MeM",
        "ChIJsc7t1Xx1dEgRhP0o62CmaDc",
        "ChIJ8Y5vBoB2dEgRD46nzsYu_o4",
        "ChIJdaoLDoF2dEgRiV69LlZ3qW0",
        "ChIJWx1bxYJ2dEgRqePilfiyv7M",
        "ChIJ1cuKzBx1dEgRi3ZCVQ4kxwM",
        "ChIJ-y2EuaVxdEgRBsFYp5Dnr0c",
        "ChIJ7V_uddtxdEgR5ramL0g04Kw",
        "ChIJhWcHkzx0dEgRgGl4dLi5jPM",
        "ChIJV0VnK8trPWERCJZymYtAZns",
        "ChIJK8KekF8PdkgRfnME4cuaep4",
        "ChIJHalg2ItzdEgRz9hKI653P5s",
        "ChIJHyXPCOl1dEgRiM97ABlq4IE",
        "ChIJ8wY49zx0dEgREbaguLHvRw8",
        "ChIJQ2iyKzRxdEgRpJt7S19UE8I",
        "ChIJ__8jKDxxdEgRdem5mD49V7M",
        "ChIJk6TznedxdEgRETXfSvwRxNA",
        "ChIJc5QaNTxxdEgRX-X91UPMCEA",
        "ChIJPcV5IdlxdEgRgsALZTWe-Gk",
        "ChIJOUrTlBVxdEgRgkHmuNbjXM8",
        "ChIJC-bjtqFxdEgRosNNeGLKmlE",
        "ChIJ_3WXFilpWW0RzCLFcgshjro",
        "ChIJVVUZYW5xdEgRFeOig_sNvhc",
        "ChIJrUDpeyVxdEgRR9TPhgqCYoQ",
        "ChIJTUR-iWhxdEgRwNwddr0YFA4",
        "ChIJBSYzNzVXoQIRkcwlj5WuAQU",
        "ChIJ49lJkctxdEgRlL5eCa87cGQ",
        "ChIJd9gav7aasSARr1MFTQ87Fs4",
        "ChIJhyfWFC5ydEgRIFYETBmXkEc",
        "ChIJw1BEOIR2dEgRpF9m2G8U5s4",
        "ChIJQV2JZnPnJCwROaLHwH4fo24",
        "ChIJ2Rpb-DVxdEgRNQrGaK_BbMA",
        "ChIJiz4I2B3lz6wRp4QaGzTX6r8",
        "ChIJB5by2KF3dEgRKfTPRvrRVxo",
        "ChIJ-xkxCqL23KIRCF0FZtGes0s",
        "ChIJL97LsVpxdEgRvQS-YRXeaX4",
        "ChIJDWDamIihbUgRPDYL9m4XfI0",
        "ChIJ18vERRShbUgRfQTUGnhMLEM",
        "ChIJWVWTc0mhbUgRURQ8GOcMBds",
        "ChIJSWoIVpymbUgRwCmGNV7nVG8",
        "ChIJNUdOiEWhbUgRwHY0NPGzAKg",
        "ChIJ1Tk_MGalbUgRNiZV4SE1emk",
        "ChIJgT5N8LymbUgRQ57a6JJjMfw",
        "ChIJOSGMgUCkbUgR3HiY0ZQ5vTg",
        "ChIJRcJdoxKkbUgRbaNYGw9zob0",
        "ChIJi8H2duEl3ywR6w9gXevacKM",
        "ChIJ1dXqY75D1qsRDO_sTMOMoIs",
        "ChIJuX0ZIV0KPYIRw_3C6EPUYR8",
        "ChIJ04BMnDajsm4RzBGIMmBpOQE",
        "ChIJmeO0Bk6lbUgR9gEybCAAYv4",
        "ChIJH8iQxPanbUgRyRef7u3CgZs",
        "ChIJqRKnyPKnbUgRtQGnmLEtoY0",
        "ChIJ2bZHRaClbUgRWc32A9RLes0",
        "ChIJ7zENfZgyXowRvArU2Qe7VW4",
        "ChIJY-xvj3WvfiYRKzLIbNMBrgo",
        "ChIJuxZNG_-lbUgRCP2k68GzMnA",
        "ChIJx9HvU_Il86wRbzjL5K_UYyY",
        "ChIJ2-v2DqCjbUgR30EwWey3Tok",
        "ChIJLS0jDTIyJmERWDrt4Lg-wsQ",
        "ChIJkT6-k7cqk0IRj1RqOzm-rY4",
        "ChIJE9cCb4OmbUgRD5_lYTWxS58",
        "ChIJeVaEM-mjbUgR9PQiMVrisbo",
        "ChIJWc6QGtClbUgRhCpx0gnM6fE",
        "ChIJ53ckxQqkbUgRizy_NTeSGYQ",
        "ChIJKbXy86mnbUgR520YWPsv2TM",
        "ChIJO1jlAj-kbUgR4TOLYO8IyWk",
        "ChIJFZl41uSjbUgRXN0r22WH1N8",
        "ChIJw2OzTPqjbUgRKDhgjd_fwzw",
        "ChIJbxzjrw-yPkIRNfxWY2fBrrM",
        "ChIJ0T5MwWalbUgRzsMakaStEUI",
        "ChIJHbiZJoCmbUgRaktnP6USY80",
        "ChIJUbQWLEOXbUgRS14O-JZi6-k",
        "ChIJuS1gEQOkbUgRFVBbdCG_ALA",
        "ChIJ48W12qmmbUgRZ2wmabtUINc",
        "ChIJS1weeWukbUgRy5crmH16WYg",
        "ChIJ84PP32ykbUgRHuocRqIPWTY",
        "ChIJX3NlztGlbUgRUGyyw7vMrCE",
        "ChIJLZzcxSClbUgRE5C21yrvDSI",
        "ChIJZ-Ok4iKkbUgR7fySrByqJNs",
        "ChIJqQotxZCmbUgRuF82dEVIXeM",
        "ChIJNxUFLs-lbUgRBPi0mvcLi2Q",
        "ChIJ8QVkugqkbUgRofEszBVU8E0",
        "ChIJHfCrRBTW2mQRtVQ4dnZWlVE",
        "ChIJuYDZxXWkbUgRSo40Tzgbo74",
        "ChIJwSuR05imbUgR4FOxePXwXDU",
    ]
    max_reviews=10
    language="en"
    # Example usage with Pydantic models
    config = ScraperConfig(
        api_token=api_token,  # Replace with your actual token
        place_ids=place_ids,
        max_reviews=max_reviews,
        language=language,
    )

    # Option 1: Get just review texts as strings
    review_texts = get_google_maps_review_texts(config)

    # Save review texts to JSON
    save_reviews_to_json(review_texts, "review_texts.json")

    # Print summary
    print(f"\nTotal: {review_texts.total_reviews} reviews from {review_texts.total_places} places")
    for place_id, texts in review_texts.texts_by_place.items():
        print(f"{place_id}: {len(texts)} reviews")

    # Option 2: Get full review data with all fields (uncomment if needed)
    # reviews = get_google_maps_reviews(config)
    # save_reviews_to_json(reviews, "google_maps_reviews.json")
    # print(f"\nTotal: {reviews.total_reviews} reviews from {reviews.total_places} places")
    #
    # # Access individual review fields
    # for place_id, place_reviews in reviews.reviews_by_place.items():
    #     print(f"\n{place_id}:")
    #     for review in place_reviews[:3]:  # Show first 3 reviews
    #         print(f"  - {review.author_name}: {review.rating} stars")
    #         print(f"    {review.text[:100]}...")


if __name__ == "__main__":
    main()
