"""
X (Twitter) Data Processor for Gen-Z Finance Bill Analysis
Process X/Twitter JSON data for analysis
"""

import pandas as pd
import json
import re
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter
import numpy as np

class XDataProcessor:
    def __init__(self, json_file_path):
        """
        Initialize processor with X/Twitter JSON file
        
        Args:
            json_file_path (str): Path to your X/Twitter JSON file
        """
        self.json_file_path = json_file_path
        self.raw_data = None
        self.processed_df = None
        self.finance_bill_hashtags = [
            '#rejectfinancebill2024', '#rutomustgo', '#occupyparliament', 
            '#genzkenya', '#kenyaprotests', '#genzrevolution', '#totalshutdown',
            '#kenyangenz', '#financebill2024', '#youth4change', '#rutoamustgo',
            '#parliamentoccupied', '#kenyageneration', '#kenyanprotest', '#genzparliament'
        ]
        
    def load_data(self):
        """Load X/Twitter JSON data"""
        try:
            with open(self.json_file_path, 'r', encoding='utf-8') as f:
                self.raw_data = json.load(f)
            
            # Handle different JSON structures
            if isinstance(self.raw_data, dict):
                # If it's a dict, look for common keys that contain tweet arrays
                if 'tweets' in self.raw_data:
                    self.raw_data = self.raw_data['tweets']
                elif 'data' in self.raw_data:
                    self.raw_data = self.raw_data['data']
                elif 'results' in self.raw_data:
                    self.raw_data = self.raw_data['results']
                else:
                    # If it's a single tweet object, make it a list
                    self.raw_data = [self.raw_data]
            
            print(f"✅ Loaded {len(self.raw_data)} X/Twitter records")
            return True
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            return False
    
    def process_data(self):
        """Process raw X/Twitter data into structured DataFrame"""
        if not self.raw_data:
            print("❌ No data loaded. Run load_data() first.")
            return None
            
        processed_records = []
        
        for record in self.raw_data:
            try:
                # Handle different tweet structures
                processed_record = {
                    # Tweet metadata
                    'tweet_id': record.get('id', record.get('id_str', '')),
                    'tweet_url': f"https://twitter.com/i/web/status/{record.get('id', record.get('id_str', ''))}",
                    'text': record.get('text', record.get('full_text', '')),
                    
                    # Author info - handle nested user object
                    'author_username': self._get_user_field(record, 'screen_name'),
                    'author_display_name': self._get_user_field(record, 'name'),
                    'author_followers': self._get_user_field(record, 'followers_count'),
                    'author_verified': self._get_user_field(record, 'verified'),
                    'author_id': self._get_user_field(record, 'id_str'),
                    
                    # Engagement metrics
                    'likes': record.get('favorite_count', record.get('favourites_count', 0)),
                    'retweets': record.get('retweet_count', 0),
                    'replies': record.get('reply_count', 0),
                    'quotes': record.get('quote_count', 0),
                    
                    # Timing
                    'created_at': record.get('created_at', ''),
                    'created_timestamp': self._parse_twitter_date(record.get('created_at', '')),
                    
                    # Tweet type
                    'is_retweet': record.get('retweeted_status') is not None,
                    'is_quote': record.get('quoted_status') is not None,
                    'is_reply': record.get('in_reply_to_status_id') is not None,
                    
                    # Location
                    'location': self._get_user_field(record, 'location'),
                    'geo_coordinates': record.get('geo', {}).get('coordinates', ''),
                    
                    # Media and entities
                    'has_media': self._has_media(record),
                    'media_count': self._count_media(record),
                    'hashtag_count': self._count_hashtags(record),
                    'mention_count': self._count_mentions(record),
                    'url_count': self._count_urls(record),
                    
                    # Language
                    'language': record.get('lang', ''),
                    
                    # Source
                    'source': record.get('source', ''),
                }
                
                processed_records.append(processed_record)
                
            except Exception as e:
                print(f"⚠️ Error processing record: {e}")
                continue
        
        self.processed_df = pd.DataFrame(processed_records)
        print(f"✅ Processed {len(self.processed_df)} records")
        return self.processed_df
    
    def _get_user_field(self, record, field):
        """Extract user field from various possible locations"""
        # Direct user object
        if 'user' in record and isinstance(record['user'], dict):
            return record['user'].get(field, '')
        
        # Direct field (some APIs flatten user data)
        if field in record:
            return record[field]
        
        # Author object
        if 'author' in record and isinstance(record['author'], dict):
            return record['author'].get(field, '')
        
        return ''
    
    def _parse_twitter_date(self, date_str):
        """Parse Twitter date format to datetime"""
        if not date_str:
            return None
        try:
            # Twitter format: "Wed Oct 05 20:30:00 +0000 2022"
            return datetime.strptime(date_str, "%a %b %d %H:%M:%S %z %Y")
        except:
            try:
                # ISO format
                return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            except:
                return None
    
    def _has_media(self, record):
        """Check if tweet has media"""
        entities = record.get('entities', {})
        extended_entities = record.get('extended_entities', {})
        
        media = entities.get('media', []) + extended_entities.get('media', [])
        return len(media) > 0
    
    def _count_media(self, record):
        """Count media items in tweet"""
        entities = record.get('entities', {})
        extended_entities = record.get('extended_entities', {})
        
        media = entities.get('media', []) + extended_entities.get('media', [])
        return len(media)
    
    def _count_hashtags(self, record):
        """Count hashtags in tweet"""
        entities = record.get('entities', {})
        hashtags = entities.get('hashtags', [])
        return len(hashtags)
    
    def _count_mentions(self, record):
        """Count mentions in tweet"""
        entities = record.get('entities', {})
        mentions = entities.get('user_mentions', [])
        return len(mentions)
    
    def _count_urls(self, record):
        """Count URLs in tweet"""
        entities = record.get('entities', {})
        urls = entities.get('urls', [])
        return len(urls)
    
    def extract_hashtags(self):
        """Extract hashtags from tweet text"""
        if self.processed_df is None:
            print("❌ No processed data. Run process_data() first.")
            return None
            
        def get_hashtags(text):
            if pd.isna(text):
                return []
            hashtags = re.findall(r'#\w+', str(text).lower())
            return hashtags
        
        self.processed_df['hashtags'] = self.processed_df['text'].apply(get_hashtags)
        
        # Filter for Finance Bill related content
        def has_finance_bill_hashtag(hashtag_list):
            return any(hashtag in self.finance_bill_hashtags for hashtag in hashtag_list)
        
        self.processed_df['finance_bill_related'] = self.processed_df['hashtags'].apply(has_finance_bill_hashtag)
        
        # Also check text content for keywords
        finance_keywords = ['finance bill', 'ruto must go', 'occupy parliament', 'gen z', 'kenya protest']
        def has_finance_keywords(text):
            if pd.isna(text):
                return False
            text_lower = str(text).lower()
            return any(keyword in text_lower for keyword in finance_keywords)
        
        self.processed_df['finance_bill_keywords'] = self.processed_df['text'].apply(has_finance_keywords)
        
        # Combine hashtag and keyword filters
        self.processed_df['finance_bill_content'] = (
            self.processed_df['finance_bill_related'] | 
            self.processed_df['finance_bill_keywords']
        )
        
        finance_bill_df = self.processed_df[self.processed_df['finance_bill_content'] == True]
        print(f"✅ Found {len(finance_bill_df)} Finance Bill related tweets")
        
        return finance_bill_df
    
    def analyze_hashtags(self):
        """Analyze hashtag patterns and frequency"""
        if self.processed_df is None:
            return None
            
        # Get all hashtags
        all_hashtags = []
        for hashtag_list in self.processed_df['hashtags']:
            all_hashtags.extend(hashtag_list)
        
        # Count frequencies
        hashtag_counts = Counter(all_hashtags)
        
        # Focus on Finance Bill hashtags
        finance_bill_counts = {hashtag: count for hashtag, count in hashtag_counts.items() 
                              if hashtag in self.finance_bill_hashtags}
        
        print("📊 Finance Bill Hashtag Analysis:")
        for hashtag, count in sorted(finance_bill_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"   {hashtag}: {count} tweets")
        
        return hashtag_counts, finance_bill_counts
    
    def get_top_influencers(self, n=50):
        """Get top influencers by engagement metrics"""
        if self.processed_df is None:
            return None
            
        # Filter for Finance Bill content
        finance_df = self.processed_df[self.processed_df['finance_bill_content'] == True]
        
        # Group by author and calculate metrics
        influencer_stats = finance_df.groupby('author_username').agg({
            'likes': 'sum',
            'retweets': 'sum', 
            'replies': 'sum',
            'quotes': 'sum',
            'tweet_id': 'count',
            'author_followers': 'first',
            'author_display_name': 'first',
            'author_verified': 'first'
        }).rename(columns={'tweet_id': 'tweet_count'})
        
        # Calculate engagement metrics
        influencer_stats['total_engagement'] = (
            influencer_stats['likes'] + 
            influencer_stats['retweets'] + 
            influencer_stats['replies'] + 
            influencer_stats['quotes']
        )
        
        influencer_stats['avg_engagement_per_tweet'] = (
            influencer_stats['total_engagement'] / influencer_stats['tweet_count']
        )
        
        # Sort by total engagement
        top_influencers = influencer_stats.sort_values('total_engagement', ascending=False).head(n)
        
        print(f"🎯 Top {n} Finance Bill X/Twitter Influencers:")
        for i, (username, stats) in enumerate(top_influencers.iterrows(), 1):
            verified_mark = "✅" if stats['author_verified'] else ""
            print(f"   {i:2d}. @{username} {verified_mark}: {stats['tweet_count']} tweets, "
                  f"{stats['total_engagement']:,} total engagement, "
                  f"{stats['author_followers']:,} followers")
        
        return top_influencers
    
    def timeline_analysis(self):
        """Analyze posting timeline and activity patterns"""
        if self.processed_df is None:
            return None
            
        # Convert timestamps
        self.processed_df['created_date'] = pd.to_datetime(self.processed_df['created_timestamp'])
        
        # Filter for Finance Bill content
        finance_df = self.processed_df[self.processed_df['finance_bill_content'] == True]
        
        if len(finance_df) == 0:
            print("❌ No Finance Bill content found for timeline analysis")
            return None
        
        # Daily activity
        daily_activity = finance_df.groupby(finance_df['created_date'].dt.date).size()
        
        # Hourly activity
        hourly_activity = finance_df.groupby(finance_df['created_date'].dt.hour).size()
        
        print("📅 Timeline Analysis:")
        print(f"   Date range: {daily_activity.index.min()} to {daily_activity.index.max()}")
        print(f"   Peak day: {daily_activity.idxmax()} ({daily_activity.max()} tweets)")
        print(f"   Total days active: {len(daily_activity)}")
        print(f"   Peak hour: {hourly_activity.idxmax()}:00 ({hourly_activity.max()} tweets)")
        
        return daily_activity, hourly_activity
    
    def engagement_analysis(self):
        """Analyze engagement patterns"""
        if self.processed_df is None:
            return None
            
        finance_df = self.processed_df[self.processed_df['finance_bill_content'] == True]
        
        print("📊 Engagement Analysis:")
        print(f"   Most liked tweet: {finance_df['likes'].max():,} likes")
        print(f"   Most retweeted: {finance_df['retweets'].max():,} retweets")
        print(f"   Most replied to: {finance_df['replies'].max():,} replies")
        
        # Tweet types analysis
        tweet_types = {
            'Original tweets': len(finance_df[~finance_df['is_retweet'] & ~finance_df['is_reply']]),
            'Retweets': len(finance_df[finance_df['is_retweet']]),
            'Replies': len(finance_df[finance_df['is_reply']]),
            'Quotes': len(finance_df[finance_df['is_quote']])
        }
        
        print("\n📊 Tweet Types:")
        for tweet_type, count in tweet_types.items():
            print(f"   {tweet_type}: {count} ({count/len(finance_df)*100:.1f}%)")
        
        return tweet_types
    
    def create_summary_report(self):
        """Create comprehensive summary report"""
        if self.processed_df is None:
            print("❌ No data processed yet.")
            return None
            
        print("\n" + "="*60)
        print("🐦 X/TWITTER FINANCE BILL ANALYSIS SUMMARY")
        print("="*60)
        
        # Basic stats
        total_tweets = len(self.processed_df)
        finance_tweets = len(self.processed_df[self.processed_df['finance_bill_content'] == True])
        
        print(f"📊 Dataset Overview:")
        print(f"   Total tweets: {total_tweets:,}")
        print(f"   Finance Bill related: {finance_tweets:,} ({finance_tweets/total_tweets*100:.1f}%)")
        
        # Engagement stats
        finance_df = self.processed_df[self.processed_df['finance_bill_content'] == True]
        if len(finance_df) > 0:
            print(f"\n🎯 Finance Bill Content Engagement:")
            print(f"   Total likes: {finance_df['likes'].sum():,}")
            print(f"   Total retweets: {finance_df['retweets'].sum():,}")
            print(f"   Total replies: {finance_df['replies'].sum():,}")
            print(f"   Total quotes: {finance_df['quotes'].sum():,}")
            print(f"   Average likes per tweet: {finance_df['likes'].mean():.1f}")
            print(f"   Average retweets per tweet: {finance_df['retweets'].mean():.1f}")
            
            # Language analysis
            lang_counts = finance_df['language'].value_counts().head(5)
            print(f"\n🌍 Top Languages:")
            for lang, count in lang_counts.items():
                print(f"   {lang}: {count} tweets ({count/len(finance_df)*100:.1f}%)")
        
        # Top hashtags
        print(f"\n🔥 Hashtag Analysis:")
        self.analyze_hashtags()
        
        # Top influencers
        print(f"\n👥 Top Influencers:")
        self.get_top_influencers(20)
        
        # Timeline
        print(f"\n📅 Timeline Analysis:")
        self.timeline_analysis()
        
        # Engagement patterns
        print(f"\n📊 Engagement Patterns:")
        self.engagement_analysis()
        
        return {
            'total_tweets': total_tweets,
            'finance_tweets': finance_tweets,
            'processed_df': self.processed_df,
            'finance_df': finance_df
        }
    
    def export_influencer_list(self, filename="finance_bill_influencers.csv"):
        """Export list of influencers for further analysis"""
        if self.processed_df is None:
            return None
            
        top_influencers = self.get_top_influencers(100)  # Get top 100
        
        # Add additional metrics
        top_influencers['engagement_rate'] = (
            top_influencers['total_engagement'] / 
            top_influencers['author_followers'] * 100
        )
        
        # Save to CSV
        export_path = f"data/processed_data/{filename}"
        top_influencers.to_csv(export_path)
        print(f"✅ Influencer list exported to {export_path}")
        
        return top_influencers

# Usage Example
if __name__ == "__main__":
    # Initialize processor
    processor = XDataProcessor("data/scraped_data/your_x_file.json")
    
    # Process data
    if processor.load_data():
        df = processor.process_data()
        if df is not None:
            finance_df = processor.extract_hashtags()
            summary = processor.create_summary_report()
            
            # Save processed data
            processor.processed_df.to_csv("data/processed_data/x_processed.csv", index=False)
            finance_df.to_csv("data/processed_data/x_finance_bill.csv", index=False)
            
            # Export influencer list
            processor.export_influencer_list()
            
            print("\n✅ Processing complete! Files saved to data/processed_data/")
            print("📁 Files created:")
            print("   - x_processed.csv (all processed tweets)")
            print("   - x_finance_bill.csv (Finance Bill related tweets)")
            print("   - finance_bill_influencers.csv (Top influencers)")
        else:
            print("❌ Failed to process data.")
    else:
        print("❌ Failed to load data. Check your file path.")