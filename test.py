import streamlit as st

# THIS MUST BE THE FIRST STREAMLIT COMMAND
st.set_page_config(page_title="Reputation Monitor", layout="wide")

import pandas as pd
import requests
from apify_client import ApifyClient
from textblob import TextBlob
import praw
import nltk
from pymongo import MongoClient
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import schedule
import time
import threading
import io
import csv
from bson.json_util import dumps
import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from dotenv import load_dotenv
from bson import ObjectId

# Load environment variables
load_dotenv()

# Initialize NLTK sentiment analyzer
try:
    nltk.download("vader_lexicon", quiet=True)
    sid = SentimentIntensityAnalyzer()
    nltk_initialized = True
    nltk_error = None
except Exception as e:
    nltk_initialized = False
    nltk_error = str(e)

# --- Constants ---
# Fix: Use environment variables with fallbacks
APIFY_TOKEN = os.getenv('APIFY_TOKEN', 'apify_api_PM0WSSaO4ZNAQJ1K8oXm0WY4p5DQvi2zet9l')
SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY', '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')
SENDER_EMAIL = os.getenv('SENDER_EMAIL', 'adityabh1business@gmail.com')
MONGO_URI = os.getenv('MONGO_URI', 'mongodb+srv://adityabh1business:e3khMbOXXZNE7cFB@orm.rttvbxf.mongodb.net/orm?retryWrites=true&w=majority&appName=ORM')

# Reddit API credentials
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID', 'SKCN5KrUqaKbG2jJIBtGFA')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET', 'nhJi28z1mBqRGDxbIxfHmpEzoXzTyg')
REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT', 'scraper/0.1 by NoLeadership6393')

current_apify_index = 0

# --- MongoDB Client Setup ---
mongo_client = None
db = None
collection = None

def initialize_mongodb():
    global mongo_client, db, collection
    try:
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        
        # Test the connection
        mongo_client.admin.command('ismaster')
        
        db = mongo_client["test"]  # Fixed: Use correct database name
        collection = db["reputations"]
        
        return True, "MongoDB connection established successfully!"
        
    except Exception as e:
        return False, f"MongoDB connection failed: {e}"

# Initialize MongoDB connection
mongodb_connected, mongodb_message = initialize_mongodb()

# --- Enhanced MongoDB Data Fetching ---
def fetch_mongodb_data():
    """Fetch all users from MongoDB collection"""
    try:
        if not mongodb_connected:
            return [], "MongoDB not connected"
            
        # Fetch all users from MongoDB
        users = list(collection.find({}))
        
        if not users:
            return [], "No users found in the database"
            
        return users, f"Found {len(users)} users in MongoDB"
        
    except Exception as e:
        return [], f"Error fetching data from MongoDB: {e}"

def export_mongodb_to_csv():
    """Export MongoDB data to CSV format for processing"""
    try:
        users, message = fetch_mongodb_data()
        
        if not users:
            return None, message
            
        # Flatten nested fields for CSV format
        flattened_users = []
        for user in users:
            # Handle nested google data
            google_data = user.get("google", [])
            google_url = ""
            google_keywords = ""
            if isinstance(google_data, list) and google_data:
                google_url = google_data[0].get("url", "")
                google_keywords = ",".join(google_data[0].get("keywords", []))
            elif isinstance(google_data, dict):
                google_url = google_data.get("url", "")
                google_keywords = ",".join(google_data.get("keywords", []))
            
            # Handle nested instagram data
            instagram_data = user.get("instagram", [])
            instagram_url = ""
            instagram_keywords = ""
            if isinstance(instagram_data, list) and instagram_data:
                instagram_url = instagram_data[0].get("url", "")
                instagram_keywords = ",".join(instagram_data[0].get("keywords", []))
            elif isinstance(instagram_data, dict):
                instagram_url = instagram_data.get("url", "")
                instagram_keywords = ",".join(instagram_data.get("keywords", []))
            
            # Handle nested twitter data
            twitter_data = user.get("twitter", [])
            twitter_tweet_id = ""
            twitter_keywords = ""
            if isinstance(twitter_data, list) and twitter_data:
                twitter_tweet_id = twitter_data[0].get("tweet_id", "")
                twitter_keywords = ",".join(twitter_data[0].get("keywords", []))
            elif isinstance(twitter_data, dict):
                twitter_tweet_id = twitter_data.get("tweet_id", "")
                twitter_keywords = ",".join(twitter_data.get("keywords", []))
            
            flat_user = {
                "email": user.get("email", ""),
                "brand": user.get("brand", ""),
                "google_url": google_url,
                "google_keywords": google_keywords,
                "reddit_keywords": user.get("reddit", ""),
                "instagram_url": instagram_url,
                "instagram_keywords": instagram_keywords,
                "twitter_tweet_id": twitter_tweet_id,
                "twitter_keywords": twitter_keywords
            }
            flattened_users.append(flat_user)
        
        # Convert to DataFrame
        df = pd.DataFrame(flattened_users)
        
        # Remove MongoDB's internal _id field if present
        if '_id' in df.columns:
            df.drop(columns=['_id'], inplace=True)
        
        return df, message
        
    except Exception as e:
        return None, f"Error converting MongoDB data to DataFrame: {e}"

# --- Test MongoDB Connection ---
def test_mongodb_connection():
    try:
        if not mongodb_connected:
            return False, "MongoDB not initialized"
            
        # Test connection by pinging
        mongo_client.admin.command('ping')
        
        # Count documents in collection
        doc_count = collection.count_documents({})
        
        return True, f"Connection successful. Found {doc_count} documents in collection."
        
    except Exception as e:
        return False, f"Connection test failed: {e}"

# --- Apify Client Setup ---
def get_apify_client():
    """Get Apify client with fallback mechanism"""
    global current_apify_index
    
    # Handle single token or multiple tokens
    if isinstance(APIFY_TOKEN, str):
        tokens = [t.strip() for t in APIFY_TOKEN.split(",") if t.strip()]
    else:
        tokens = [APIFY_TOKEN]
    
    for _ in range(len(tokens)):
        try:
            client = ApifyClient(tokens[current_apify_index])
            # Test the token by getting user info
            client.user().get()
            return client
        except Exception:
            current_apify_index = (current_apify_index + 1) % len(tokens)
    
    raise Exception("All Apify tokens failed.")

# --- Sentiment Function ---
def get_sentiment(text):
    """Get sentiment of text using VADER analyzer"""
    if not text or not isinstance(text, str):
        return "Neutral"
    try:
        if nltk_initialized:
            score = sid.polarity_scores(text)["compound"]
        else:
            # Fallback to TextBlob if NLTK fails
            score = TextBlob(text).sentiment.polarity
        
        if score > 0.3:
            return "Positive"
        elif score < -0.3:
            return "Negative"
        else:
            return "Neutral"
    except Exception:
        return "Neutral"

# --- Scrape Google Reviews with Enhanced Error Handling ---
def scrape_google_reviews_apify(place_url, keywords):
    """Scrape Google Reviews using Apify"""
    if not place_url or not keywords:
        return []
        
    keywords_list = [k.strip() for k in keywords.split(",") if k.strip()]
    if not keywords_list:
        return []
        
    try:
        st.write(f"🔍 Scraping Google Reviews for URL: {place_url[:50]}...")
        apify_client = get_apify_client()
        run_input = {
            "startUrls": [{"url": place_url}],
            "maxReviews": 100,
            "reviewsSort": "newest",
            "language": "en",
            "reviewsOrigin": "all",
            "personalData": True,
        }
        run = apify_client.actor("Xb8osYTtOjlsgI6k9").call(run_input=run_input)
        raw_reviews = list(apify_client.dataset(run["defaultDatasetId"]).iterate_items())
        
        st.write(f"✅ Found {len(raw_reviews)} total Google reviews")
        
    except Exception as e:
        st.error(f"Google Reviews API error: {e}")
        return []

    filtered_reviews = []
    for review in raw_reviews:
        text = review.get("text")
        if not text or not isinstance(text, str):
            continue
        if any(kw.lower() in text.lower() for kw in keywords_list):
            sentiment = get_sentiment(text)
            filtered_reviews.append({
                "text": text,
                "sentiment": sentiment,
                "polarity": TextBlob(text).sentiment.polarity,
                "rating": review.get("stars", "N/A"),
                "date": review.get("publishedAtDate", "N/A")
            })
    
    st.write(f"🎯 Filtered {len(filtered_reviews)} relevant Google reviews")
    return filtered_reviews

# --- Scrape Reddit with Enhanced Functionality ---
def scrape_reddit_mentions(firm_name, keywords):
    """Scrape Reddit mentions using PRAW"""
    if not firm_name or not keywords:
        return []
        
    keywords_list = [k.strip() for k in keywords.split(",") if k.strip()]
    if not keywords_list:
        return []
        
    try:
        st.write(f"🔍 Scraping Reddit for brand: {firm_name}...")
        reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT
        )
        
        all_mentions = []
        search_terms = [firm_name] + keywords_list
        
        for term in search_terms:
            try:
                for submission in reddit.subreddit("all").search(term, limit=25, sort="new"):
                    content = submission.title + "\n" + submission.selftext
                    if any(k.lower() in content.lower() for k in keywords_list):
                        sentiment = get_sentiment(content)
                        all_mentions.append({
                            "url": submission.url,
                            "title": submission.title,
                            "content": content[:200] + "...",
                            "sentiment": sentiment,
                            "score": submission.score,
                            "subreddit": submission.subreddit.display_name
                        })
            except Exception as e:
                st.warning(f"Error searching for term '{term}': {e}")
                continue
        
        st.write(f"✅ Found {len(all_mentions)} Reddit mentions")
        return all_mentions
        
    except Exception as e:
        st.error(f"Reddit API error: {e}")
        return []

# --- Scrape Instagram Comments with Enhanced Features ---
def scrape_instagram_comments(url, keywords):
    """Scrape Instagram comments using Apify"""
    if not url or not keywords:
        return []
        
    keywords_list = [k.strip() for k in keywords.split(",") if k.strip()]
    if not keywords_list:
        return []
        
    try:
        st.write(f"🔍 Scraping Instagram comments for URL: {url[:50]}...")
        apify_client = get_apify_client()
        run_input = {
            "directUrls": [url] if url else [],
            "resultsLimit": 50,
            "includeNestedComments": True,
            "isNewestComments": True
        }
        run = apify_client.actor("SbK00X0JYCPblD2wp").call(run_input=run_input)
        comments = []
        
        for item in apify_client.dataset(run["defaultDatasetId"]).iterate_items():
            comment_text = item.get("text", "")
            if any(kw.lower() in comment_text.lower() for kw in keywords_list):
                sentiment = get_sentiment(comment_text)
                comments.append({
                    "text": comment_text,
                    "sentiment": sentiment,
                    "likes": item.get("likesCount", 0),
                    "username": item.get("ownerUsername", "Unknown")
                })
        
        st.write(f"✅ Found {len(comments)} relevant Instagram comments")
        return comments
        
    except Exception as e:
        st.error(f"Instagram API error: {e}")
        return []

# --- Scrape Twitter Comments with Enhanced Features ---
def scrape_twitter_comments(tweet_id, keywords):
    """Scrape Twitter comments using Apify"""
    if not tweet_id or not keywords:
        return []
        
    keywords_list = [k.strip() for k in keywords.split(",") if k.strip()]
    if not keywords_list:
        return []
        
    try:
        st.write(f"🔍 Scraping Twitter comments for Tweet ID: {tweet_id}...")
        apify_client = get_apify_client()
        run_input = {
            "tweetIDs": [tweet_id] if tweet_id else [],
            "searchTerms": keywords_list,
            "maxItems": 50,
            "lang": "en",
            "from": None,
            "filter:verified": False
        }
        run = apify_client.actor("CJdippxWmn9uRfooo").call(run_input=run_input)
        results = []
        
        for item in apify_client.dataset(run["defaultDatasetId"]).iterate_items():
            comment_text = item.get("text", "")
            sentiment = get_sentiment(comment_text)
            results.append({
                "text": comment_text,
                "sentiment": sentiment,
                "retweets": item.get("retweetCount", 0),
                "likes": item.get("likeCount", 0),
                "username": item.get("author", {}).get("userName", "Unknown")
            })
        
        st.write(f"✅ Found {len(results)} relevant Twitter comments")
        return results
        
    except Exception as e:
        st.error(f"Twitter API error: {e}")
        return []

# --- Enhanced SendGrid Email Function ---
def send_email_sendgrid(to_email, subject, html_content):
    """Send email using Twilio SendGrid API"""
    try:
        # Create the email message
        message = Mail(
            from_email=SENDER_EMAIL,
            to_emails=to_email,
            subject=subject,
            html_content=html_content
        )
        
        # Initialize client and send the email
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        
        return response.status_code, f"Email sent successfully! Status code: {response.status_code}"
        
    except Exception as e:
        return 500, f"Email send failed: {e}"

# --- Enhanced HTML Report Generator ---
def generate_html_report(user_results):
    """Generate detailed HTML report for email"""
    brand = user_results.get('brand', 'Unknown Brand')
    google_reviews = user_results.get('google_reviews', [])
    reddit_results = user_results.get('reddit_results', [])
    instagram_comments = user_results.get('instagram_comments', [])
    twitter_comments = user_results.get('twitter_comments', [])
    
    # Calculate sentiment summary
    all_sentiments = []
    for review in google_reviews:
        all_sentiments.append(review.get('sentiment', 'Neutral'))
    for comment in instagram_comments:
        all_sentiments.append(comment.get('sentiment', 'Neutral'))
    for comment in twitter_comments:
        all_sentiments.append(comment.get('sentiment', 'Neutral'))
    for mention in reddit_results:
        all_sentiments.append(mention.get('sentiment', 'Neutral'))
    
    positive_count = all_sentiments.count('Positive')
    negative_count = all_sentiments.count('Negative')
    neutral_count = all_sentiments.count('Neutral')
    
    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            .header {{ background-color: #f4f4f4; padding: 20px; text-align: center; }}
            .summary {{ background-color: #e8f4fd; padding: 15px; margin: 20px 0; }}
            .section {{ margin: 20px 0; }}
            .positive {{ color: #28a745; }}
            .negative {{ color: #dc3545; }}
            .neutral {{ color: #6c757d; }}
            .metric {{ display: inline-block; margin: 10px; padding: 10px; background-color: #f8f9fa; border-radius: 5px; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h2>🔍 Reputation Report for <strong>{brand}</strong></h2>
            <p>Generated on {time.strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>
        
        <div class="summary">
            <h3>📊 Summary</h3>
            <div class="metric">
                <strong>Total Mentions:</strong> {len(all_sentiments)}
            </div>
            <div class="metric positive">
                <strong>Positive:</strong> {positive_count}
            </div>
            <div class="metric negative">
                <strong>Negative:</strong> {negative_count}
            </div>
            <div class="metric neutral">
                <strong>Neutral:</strong> {neutral_count}
            </div>
        </div>
        
        <div class="section">
            <h3>🔍 Google Reviews ({len(google_reviews)} found)</h3>
            <ul>
    """
    
    for review in google_reviews[:10]:  # Limit to first 10
        sentiment_class = review['sentiment'].lower()
        html += f"""
                <li class="{sentiment_class}">
                    <strong>Rating:</strong> {review.get('rating', 'N/A')} | 
                    <strong>Sentiment:</strong> {review['sentiment']} | 
                    <strong>Date:</strong> {review.get('date', 'N/A')}<br>
                    <em>"{review['text'][:150]}..."</em>
                </li>
        """
    
    html += f"""
            </ul>
        </div>
        
        <div class="section">
            <h3>👽 Reddit Mentions ({len(reddit_results)} found)</h3>
            <ul>
    """
    
    for mention in reddit_results[:10]:  # Limit to first 10
        sentiment_class = mention['sentiment'].lower()
        html += f"""
                <li class="{sentiment_class}">
                    <strong>Subreddit:</strong> r/{mention.get('subreddit', 'Unknown')} | 
                    <strong>Score:</strong> {mention.get('score', 0)} | 
                    <strong>Sentiment:</strong> {mention['sentiment']}<br>
                    <strong>Title:</strong> {mention.get('title', 'No title')}<br>
                    <a href="{mention['url']}" target="_blank">View Post</a>
                </li>
        """
    
    html += f"""
            </ul>
        </div>
        
        <div class="section">
            <h3>📸 Instagram Comments ({len(instagram_comments)} found)</h3>
            <ul>
    """
    
    for comment in instagram_comments[:10]:  # Limit to first 10
        sentiment_class = comment['sentiment'].lower()
        html += f"""
                <li class="{sentiment_class}">
                    <strong>User:</strong> @{comment.get('username', 'Unknown')} | 
                    <strong>Likes:</strong> {comment.get('likes', 0)} | 
                    <strong>Sentiment:</strong> {comment['sentiment']}<br>
                    <em>"{comment['text'][:150]}..."</em>
                </li>
        """
    
    html += f"""
            </ul>
        </div>
        
        <div class="section">
            <h3>🐦 Twitter Comments ({len(twitter_comments)} found)</h3>
            <ul>
    """
    
    for comment in twitter_comments[:10]:  # Limit to first 10
        sentiment_class = comment['sentiment'].lower()
        html += f"""
                <li class="{sentiment_class}">
                    <strong>User:</strong> @{comment.get('username', 'Unknown')} | 
                    <strong>Likes:</strong> {comment.get('likes', 0)} | 
                    <strong>Retweets:</strong> {comment.get('retweets', 0)} | 
                    <strong>Sentiment:</strong> {comment['sentiment']}<br>
                    <em>"{comment['text'][:150]}..."</em>
                </li>
        """
    
    html += """
            </ul>
        </div>
        
        <div style="margin-top: 40px; text-align: center; color: #6c757d;">
            <p>Thanks for using our Reputation Monitor service!</p>
            <p>This is an automated report. For questions, please contact support.</p>
        </div>
    </body>
    </html>
    """
    
    return html

# --- Save Results to Enhanced CSV ---
def save_results_to_csv(results):
    """Save results to CSV format"""
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Write header
    writer.writerow([
        "Email", "Brand", "Platform", "Content_Type", "Content", "Sentiment", 
        "Additional_Info", "URL_or_ID", "Engagement", "Date"
    ])
    
    # Write data rows
    for result in results:
        email = result.get("email", "")
        brand = result.get("brand", "")
        
        # Google Reviews
        for review in result.get("google_reviews", []):
            writer.writerow([
                email, brand, "Google", "Review", review["text"][:500], 
                review["sentiment"], f"Rating: {review.get('rating', 'N/A')}", 
                "", "", review.get('date', 'N/A')
            ])
        
        # Reddit Mentions
        for mention in result.get("reddit_results", []):
            writer.writerow([
                email, brand, "Reddit", "Post", mention.get("title", "")[:500], 
                mention["sentiment"], f"Score: {mention.get('score', 0)}", 
                mention["url"], mention.get('score', 0), "N/A"
            ])
        
        # Instagram Comments
        for comment in result.get("instagram_comments", []):
            writer.writerow([
                email, brand, "Instagram", "Comment", comment["text"][:500], 
                comment["sentiment"], f"User: @{comment.get('username', 'Unknown')}", 
                "", comment.get('likes', 0), "N/A"
            ])
        
        # Twitter Comments
        for comment in result.get("twitter_comments", []):
            writer.writerow([
                email, brand, "Twitter", "Comment", comment["text"][:500], 
                comment["sentiment"], f"User: @{comment.get('username', 'Unknown')}", 
                "", f"Likes: {comment.get('likes', 0)}, RTs: {comment.get('retweets', 0)}", "N/A"
            ])
    
    return output.getvalue()

# --- Main Processing Function ---
def process_mongodb_data_and_send_reports():
    """Main function to process MongoDB data and send reports"""
    if not mongodb_connected:
        st.error("❌ MongoDB not connected. Cannot process data.")
        return None
    
    st.info("🔄 Starting reputation monitoring process...")
    
    # Fetch data from MongoDB
    df, message = export_mongodb_to_csv()
    if df is None:
        st.error(f"❌ Failed to fetch data from MongoDB: {message}")
        return None
    
    st.success(f"✅ {message}")
    st.info(f"📊 Processing {len(df)} users...")
    
    all_results = []
    progress_bar = st.progress(0)
    
    for i, (_, user) in enumerate(df.iterrows()):
        st.write(f"🔄 Processing user {i+1}/{len(df)}: {user.get('email')} for brand: {user.get('brand')}")
        
        try:
            # Process Google Reviews
            google_reviews = scrape_google_reviews_apify(
                user.get("google_url", ""), 
                user.get("google_keywords", "")
            )
            
            # Process Reddit mentions
            reddit_results = scrape_reddit_mentions(
                user.get("brand", ""), 
                user.get("reddit_keywords", "")
            )
            
            # Process Instagram comments
            instagram_comments = scrape_instagram_comments(
                user.get("instagram_url", ""), 
                user.get("instagram_keywords", "")
            )
            
            # Process Twitter comments
            twitter_comments = scrape_twitter_comments(
                user.get("twitter_tweet_id", ""), 
                user.get("twitter_keywords", "")
            )
            
            # Store results for this user
            user_results = {
                "email": user.get("email", ""),
                "brand": user.get("brand", ""),
                "google_reviews": google_reviews,
                "reddit_results": reddit_results,
                "instagram_comments": instagram_comments,
                "twitter_comments": twitter_comments
            }
            all_results.append(user_results)
            
            # Generate and send email report
            email = user.get("email", "")
            if email:
                html_content = generate_html_report(user_results)
                status_code, response = send_email_sendgrid(
                    email, 
                    f"Reputation Report: {user.get('brand', '')}", 
                    html_content
                )
                
                if status_code == 202:  # SendGrid uses 202 for accepted
                    st.success(f"✅ Email sent successfully to {email}")
                else:
                    st.warning(f"⚠️ Email send failed to {email}: Status {status_code}")
            else:
                st.warning(f"⚠️ No email found for user with brand: {user.get('brand', '')}")
                
        except Exception as e:
            st.error(f"❌ Error processing user {user.get('email', '')}: {e}")
            continue
        
        # Update progress bar
        progress_bar.progress((i + 1) / len(df))
    
    # Save all results to CSV
    results_csv = save_results_to_csv(all_results)
    st.success("✅ All processing complete!")
    
    # Display summary statistics
    total_mentions = sum(
        len(result.get("google_reviews", [])) + 
        len(result.get("reddit_results", [])) + 
        len(result.get("instagram_comments", [])) + 
        len(result.get("twitter_comments", []))
        for result in all_results
    )
    
    st.metric("Total Mentions Found", total_mentions)
    st.metric("Users Processed", len(all_results))
    st.metric("Emails Sent", len([r for r in all_results if r.get("email")]))
    
    return results_csv

# --- Scheduler Functions ---
def start_background_scheduler():
    """Start background scheduler for daily reports"""
    thread = threading.Thread(target=run_scheduler)
    thread.daemon = True
    thread.start()

def run_scheduler():
    """Run the scheduler in background"""
    schedule.every().day.at("23:30").do(process_mongodb_data_and_send_reports)  # Fixed time
    while True:
        schedule.run_pending()
        time.sleep(60)

# --- Display Raw MongoDB Data ---
def display_mongodb_data():
    """Display raw MongoDB data in expandable format"""
    try:
        users, message = fetch_mongodb_data()
        
        if not users:
            st.warning(message)
            return
            
        st.subheader("Raw MongoDB Data:")
        for i, user in enumerate(users):
            with st.expander(f"User {i+1}: {user.get('email', 'No email')} - {user.get('brand', 'No brand')}"):
                st.json(user)
                
    except Exception as e:
        st.error(f"❌ Error displaying MongoDB data: {e}")

# --- Test All Connections ---
def test_all_connections():
    """Test all API connections and display results"""
    st.subheader("🔧 Connection Tests")
    
    # Test MongoDB
    with st.spinner("Testing MongoDB connection..."):
        mongo_success, mongo_msg = test_mongodb_connection()
        if mongo_success:
            st.success(f"✅ MongoDB: {mongo_msg}")
        else:
            st.error(f"❌ MongoDB: {mongo_msg}")
    
    # Test Apify
    with st.spinner("Testing Apify connection..."):
        try:
            client = get_apify_client()
            user_info = client.user().get()
            st.success(f"✅ Apify: Connected as {user_info.get('username', 'Unknown')}")
        except Exception as e:
            st.error(f"❌ Apify: {e}")
    
    # Test Reddit
    with st.spinner("Testing Reddit connection..."):
        try:
            reddit = praw.Reddit(
                client_id=REDDIT_CLIENT_ID,
                client_secret=REDDIT_CLIENT_SECRET,
                user_agent=REDDIT_USER_AGENT
            )
            # Test by getting user info
            reddit.user.me()
            st.success("✅ Reddit: Connection successful")
        except Exception as e:
            st.error(f"❌ Reddit: {e}")
    
    # Test SendGrid
    with st.spinner("Testing SendGrid connection..."):
        try:
            sg = SendGridAPIClient(SENDGRID_API_KEY)
            # Test connection by getting API key info (this doesn't send an email)
            st.success("✅ SendGrid: API key configured")
        except Exception as e:
            st.error(f"❌ SendGrid: {e}")
    
    # Test NLTK
    if nltk_initialized:
        st.success("✅ NLTK: Sentiment analyzer ready")
    else:
        st.error(f"❌ NLTK: {nltk_error}")

# --- Manual Single User Test ---
def test_single_user():
    """Test reputation monitoring for a single user"""
    st.subheader("🧪 Single User Test")
    
    with st.form("single_user_test"):
        col1, col2 = st.columns(2)
        
        with col1:
            test_email = st.text_input("Email Address", placeholder="test@example.com")
            test_brand = st.text_input("Brand Name", placeholder="Your Company")
            google_url = st.text_input("Google Reviews URL", placeholder="https://maps.google.com/...")
            google_keywords = st.text_input("Google Keywords", placeholder="keyword1, keyword2")
        
        with col2:
            reddit_keywords = st.text_input("Reddit Keywords", placeholder="brand, company")
            instagram_url = st.text_input("Instagram URL", placeholder="https://instagram.com/p/...")
            instagram_keywords = st.text_input("Instagram Keywords", placeholder="keyword1, keyword2")
            twitter_tweet_id = st.text_input("Twitter Tweet ID", placeholder="1234567890")
            twitter_keywords = st.text_input("Twitter Keywords", placeholder="keyword1, keyword2")
        
        submit_test = st.form_submit_button("🚀 Run Test", use_container_width=True)
    
    if submit_test and test_email and test_brand:
        st.info("🔄 Running single user test...")
        
        try:
            # Create test user data
            test_user = {
                "email": test_email,
                "brand": test_brand,
                "google_url": google_url,
                "google_keywords": google_keywords,
                "reddit_keywords": reddit_keywords,
                "instagram_url": instagram_url,
                "instagram_keywords": instagram_keywords,
                "twitter_tweet_id": twitter_tweet_id,
                "twitter_keywords": twitter_keywords
            }
            
            # Process the test user
            google_reviews = scrape_google_reviews_apify(google_url, google_keywords)
            reddit_results = scrape_reddit_mentions(test_brand, reddit_keywords)
            instagram_comments = scrape_instagram_comments(instagram_url, instagram_keywords)
            twitter_comments = scrape_twitter_comments(twitter_tweet_id, twitter_keywords)
            
            # Store results
            user_results = {
                "email": test_email,
                "brand": test_brand,
                "google_reviews": google_reviews,
                "reddit_results": reddit_results,
                "instagram_comments": instagram_comments,
                "twitter_comments": twitter_comments
            }
            
            # Display results
            st.success("✅ Test completed successfully!")
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Google Reviews", len(google_reviews))
            with col2:
                st.metric("Reddit Mentions", len(reddit_results))
            with col3:
                st.metric("Instagram Comments", len(instagram_comments))
            with col4:
                st.metric("Twitter Comments", len(twitter_comments))
            
            # Show detailed results
            if google_reviews:
                with st.expander(f"📍 Google Reviews ({len(google_reviews)})"):
                    for review in google_reviews[:5]:
                        st.write(f"**Rating:** {review.get('rating', 'N/A')} | **Sentiment:** {review['sentiment']}")
                        st.write(f"*{review['text'][:200]}...*")
                        st.divider()
            
            if reddit_results:
                with st.expander(f"👽 Reddit Mentions ({len(reddit_results)})"):
                    for mention in reddit_results[:5]:
                        st.write(f"**r/{mention.get('subreddit', 'Unknown')}** | **Score:** {mention.get('score', 0)} | **Sentiment:** {mention['sentiment']}")
                        st.write(f"*{mention.get('title', 'No title')}*")
                        st.divider()
            
            if instagram_comments:
                with st.expander(f"📸 Instagram Comments ({len(instagram_comments)})"):
                    for comment in instagram_comments[:5]:
                        st.write(f"**@{comment.get('username', 'Unknown')}** | **Likes:** {comment.get('likes', 0)} | **Sentiment:** {comment['sentiment']}")
                        st.write(f"*{comment['text'][:200]}...*")
                        st.divider()
            
            if twitter_comments:
                with st.expander(f"🐦 Twitter Comments ({len(twitter_comments)})"):
                    for comment in twitter_comments[:5]:
                        st.write(f"**@{comment.get('username', 'Unknown')}** | **Likes:** {comment.get('likes', 0)} | **Sentiment:** {comment['sentiment']}")
                        st.write(f"*{comment['text'][:200]}...*")
                        st.divider()
            
            # Generate and show email preview
            html_content = generate_html_report(user_results)
            with st.expander("📧 Email Preview"):
                st.components.v1.html(html_content, height=600, scrolling=True)
            
            # Offer to send test email
            if st.button("📧 Send Test Email"):
                status_code, response = send_email_sendgrid(
                    test_email, 
                    f"Test Reputation Report: {test_brand}", 
                    html_content
                )
                
                if status_code == 202:
                    st.success(f"✅ Test email sent successfully to {test_email}")
                else:
                    st.error(f"❌ Test email failed: {response}")
            
        except Exception as e:
            st.error(f"❌ Test failed: {e}")

# --- Statistics Dashboard ---
def show_statistics_dashboard():
    """Display statistics dashboard"""
    st.subheader("📊 Statistics Dashboard")
    
    try:
        users, message = fetch_mongodb_data()
        
        if not users:
            st.warning("No data available for statistics")
            return
        
        # Basic statistics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Users", len(users))
        
        with col2:
            google_users = sum(1 for user in users if user.get("google"))
            st.metric("Google Monitoring", google_users)
        
        with col3:
            reddit_users = sum(1 for user in users if user.get("reddit"))
            st.metric("Reddit Monitoring", reddit_users)
        
        with col4:
            instagram_users = sum(1 for user in users if user.get("instagram"))
            st.metric("Instagram Monitoring", instagram_users)
        
        # Platform distribution
        st.subheader("Platform Distribution")
        platforms = {
            "Google": google_users,
            "Reddit": reddit_users,
            "Instagram": instagram_users,
            "Twitter": sum(1 for user in users if user.get("twitter"))
        }
        
        # Create a simple bar chart
        platform_df = pd.DataFrame(list(platforms.items()), columns=['Platform', 'Users'])
        st.bar_chart(platform_df.set_index('Platform'))
        
        # Recent activity (if timestamps exist)
        st.subheader("User Brands")
        brands = [user.get("brand", "Unknown") for user in users if user.get("brand")]
        if brands:
            brand_df = pd.DataFrame(brands, columns=['Brand'])
            st.dataframe(brand_df, use_container_width=True)
        
    except Exception as e:
        st.error(f"❌ Error generating statistics: {e}")

# --- Main Streamlit App ---
def main():
    """Main Streamlit application"""
    st.title("🔍 Reputation Monitor Dashboard")
    st.markdown("---")
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox(
        "Choose a page:",
        [
            "🏠 Home",
            "🚀 Run Monitor",
            "🧪 Test Single User",
            "🔧 System Tests",
            "📊 Statistics",
            "💾 View Data",
            "⚙️ Settings"
        ]
    )
    
    # Display connection status in sidebar
    st.sidebar.markdown("---")
    st.sidebar.subheader("🔗 Connection Status")
    
    if mongodb_connected:
        st.sidebar.success("✅ MongoDB Connected")
    else:
        st.sidebar.error("❌ MongoDB Disconnected")
    
    if nltk_initialized:
        st.sidebar.success("✅ NLTK Ready")
    else:
        st.sidebar.error("❌ NLTK Failed")
    
    # Main content based on selected page
    if page == "🏠 Home":
        st.header("Welcome to Reputation Monitor")
        st.markdown("""
        This application monitors your brand's reputation across multiple platforms:
        
        - **Google Reviews**: Track customer reviews and ratings
        - **Reddit**: Monitor brand mentions and discussions
        - **Instagram**: Analyze comments and engagement
        - **Twitter**: Track tweets and conversations
        
        ### Features:
        - 🤖 **Automated Monitoring**: Daily scans across all platforms
        - 📧 **Email Reports**: Detailed reports sent to your inbox
        - 😊 **Sentiment Analysis**: Positive, negative, and neutral classification
        - 📊 **Analytics Dashboard**: View trends and statistics
        - 🔍 **Keyword Filtering**: Focus on relevant mentions only
        """)
        
        # Quick stats
        if mongodb_connected:
            users, _ = fetch_mongodb_data()
            if users:
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Users", len(users))
                with col2:
                    active_monitors = sum(1 for user in users if any([
                        user.get("google"), user.get("reddit"), 
                        user.get("instagram"), user.get("twitter")
                    ]))
                    st.metric("Active Monitors", active_monitors)
                with col3:
                    brands = len(set(user.get("brand") for user in users if user.get("brand")))
                    st.metric("Unique Brands", brands)
    
    elif page == "🚀 Run Monitor":
        st.header("Run Reputation Monitor")
        st.markdown("Process all users in the database and send email reports.")
        
        if not mongodb_connected:
            st.error("❌ MongoDB connection required to run monitor")
            return
        
        # Display current data summary
        df, message = export_mongodb_to_csv()
        if df is not None:
            st.info(f"📊 Ready to process {len(df)} users")
            
            # Show preview of data
            with st.expander("Preview User Data"):
                st.dataframe(df.head(), use_container_width=True)
            
            if st.button("🚀 Start Monitoring Process", type="primary"):
                results_csv = process_mongodb_data_and_send_reports()
                
                if results_csv:
                    st.download_button(
                        label="📥 Download Results CSV",
                        data=results_csv,
                        file_name=f"reputation_results_{time.strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
        else:
            st.error(f"❌ {message}")
    
    elif page == "🧪 Test Single User":
        test_single_user()
    
    elif page == "🔧 System Tests":
        test_all_connections()
    
    elif page == "📊 Statistics":
        show_statistics_dashboard()
    
    elif page == "💾 View Data":
        st.header("Database Contents")
        display_mongodb_data()
    
    elif page == "⚙️ Settings":
        st.header("System Settings")
        st.markdown("### Environment Variables")
        
        # Show configuration (masked for security)
        config_data = {
            "MongoDB URI": MONGO_URI[:20] + "..." if MONGO_URI else "Not set",
            "Apify Token": "Set" if APIFY_TOKEN else "Not set",
            "SendGrid API Key": "Set" if SENDGRID_API_KEY else "Not set",
            "Sender Email": SENDER_EMAIL,
            "Reddit Client ID": "Set" if REDDIT_CLIENT_ID else "Not set"
        }
        
        for key, value in config_data.items():
            col1, col2 = st.columns([1, 2])
            with col1:
                st.write(f"**{key}:**")
            with col2:
                st.write(value)
        
        st.markdown("---")
        st.markdown("### Scheduler Settings")
        st.info("📅 Automated reports are scheduled to run daily at 23:30 UTC")
        
        if st.button("🔄 Start Background Scheduler"):
            start_background_scheduler()
            st.success("✅ Background scheduler started!")
        
        st.markdown("---")
        st.markdown("### System Information")
        st.write(f"**NLTK Status:** {'✅ Ready' if nltk_initialized else '❌ Failed'}")
        st.write(f"**MongoDB Status:** {'✅ Connected' if mongodb_connected else '❌ Disconnected'}")

# --- Run the App ---
if __name__ == "__main__":
    # Initialize the app
    main()