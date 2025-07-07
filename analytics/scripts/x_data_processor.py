"""
X (Twitter) Data Processor - Generate Processed CSV Files
Process raw X/Twitter JSON data and generate all required CSV files for analysis
"""

import pandas as pd
import json
import re
import os
from datetime import datetime
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
        self.finance_bill_df = None
        self.influencers_df = None
        
        # Finance Bill related hashtags and keywords
        self.finance_bill_hashtags = [
            '#rejectfinancebill2024', '#rutomustgo', '#occupyparliament', 
            '#genzkenya', '#kenyaprotests', '#genzrevolution', '#totalshutdown',
            '#kenyangenz', '#financebill2024', '#youth4change', '#rutoamustgo',
            '#parliamentoccupied', '#kenyageneration', '#kenyanprotest', '#genzparliament',
            '#rejectfinancebill', '#financebill', '#kenyagenx', '#genxkenya'
        ]
        
        self.finance_keywords = [
            'finance bill', 'ruto must go', 'occupy parliament', 'gen z', 'genz',
            'kenya protest', 'reject finance', 'total shutdown', 'parliament occupied',
            'zakayo', 'finance act', 'tax bill', 'taxation', 'kenyan youth'
        ]
        
        # Create output directory
        os.makedirs('data/processed_data', exist_ok=True)
    
    def load_data(self):
        """Load X/Twitter JSON data"""
        try:
            print(f"📂 Loading data from: {self.json_file_path}")
            
            with open(self.json_file_path, 'r', encoding='utf-8') as f:
                self.raw_data = json.load(f)
            
            # Handle different JSON structures
            if isinstance(self.raw_data, dict):
                # Common JSON structures from different scrapers
                possible_keys = ['tweets', 'data', 'results', 'items', 'posts']
                
                for key in possible_keys:
                    if key in self.raw_data and isinstance(self.raw_data[key], list):
                        self.raw_data = self.raw_data[key]
                        print(f"✅ Found data in '{key}' field")
                        break
                else:
                    # If no array found, treat as single tweet
                    if 'id' in self.raw_data or 'text' in self.raw_data:
                        self.raw_data = [self.raw_data]
                        print("✅ Single tweet detected, converted to list")
                    else:
                        print("❌ Could not find tweet data in JSON structure")
                        return False
            
            print(f"✅ Loaded {len(self.raw_data)} X/Twitter records")
            return True
            
        except FileNotFoundError:
            print(f"❌ File not found: {self.json_file_path}")
            return False
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON format: {e}")
            return False
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            return False
    
    def process_data(self):
        """Process raw X/Twitter data into structured DataFrame"""
        if not self.raw_data:
            print("❌ No data loaded. Run load_data() first.")
            return False
            
        processed_records = []
        skipped_records = 0
        
        print("🔄 Processing tweet data...")
        
        for i, record in enumerate(self.raw_data):
            try:
                # Extract tweet text
                tweet_text = self._extract_text(record)
                if not tweet_text:
                    skipped_records += 1
                    continue
                
                # Extract user information
                user_info = self._extract_user_info(record)
                
                # Extract engagement metrics
                engagement = self._extract_engagement(record)
                
                # Extract timing information
                timing = self._extract_timing(record)
                
                # Extract tweet metadata
                metadata = self._extract_metadata(record)
                
                # Extract entities (hashtags, mentions, etc.)
                entities = self._extract_entities(record)
                
                # Combine all data
                processed_record = {
                    # Tweet basic info
                    'tweet_id': record.get('id', record.get('id_str', f'unknown_{i}')),
                    'tweet_url': f"https://twitter.com/i/web/status/{record.get('id', record.get('id_str', ''))}",
                    'text': tweet_text,
                    
                    # User info
                    'author_username': user_info.get('username', ''),
                    'author_display_name': user_info.get('display_name', ''),
                    'author_followers': user_info.get('followers', 0),
                    'author_following': user_info.get('following', 0),
                    'author_verified': user_info.get('verified', False),
                    'author_id': user_info.get('user_id', ''),
                    'author_created_at': user_info.get('created_at', ''),
                    'location': user_info.get('location', ''),
                    
                    # Engagement metrics
                    'likes': engagement.get('likes', 0),
                    'retweets': engagement.get('retweets', 0),
                    'replies': engagement.get('replies', 0),
                    'quotes': engagement.get('quotes', 0),
                    'bookmarks': engagement.get('bookmarks', 0),
                    
                    # Timing
                    'created_at': timing.get('created_at', ''),
                    'created_timestamp': timing.get('timestamp', None),
                    'created_date': timing.get('date', ''),
                    'created_time': timing.get('time', ''),
                    'created_hour': timing.get('hour', 0),
                    'created_day_of_week': timing.get('day_of_week', ''),
                    
                    # Tweet metadata
                    'is_retweet': metadata.get('is_retweet', False),
                    'is_quote': metadata.get('is_quote', False),
                    'is_reply': metadata.get('is_reply', False),
                    'is_thread': metadata.get('is_thread', False),
                    'language': metadata.get('language', ''),
                    'source': metadata.get('source', ''),
                    'possibly_sensitive': metadata.get('possibly_sensitive', False),
                    
                    # Entities
                    'hashtags': entities.get('hashtags', []),
                    'mentions': entities.get('mentions', []),
                    'urls': entities.get('urls', []),
                    'hashtag_count': entities.get('hashtag_count', 0),
                    'mention_count': entities.get('mention_count', 0),
                    'url_count': entities.get('url_count', 0),
                    'has_media': entities.get('has_media', False),
                    'media_count': entities.get('media_count', 0),
                    'media_types': entities.get('media_types', []),
                    
                    # Calculated fields
                    'total_engagement': engagement.get('likes', 0) + engagement.get('retweets', 0) + engagement.get('replies', 0),
                    'engagement_rate': self._calculate_engagement_rate(engagement, user_info.get('followers', 0)),
                    'text_length': len(tweet_text),
                    'word_count': len(tweet_text.split()) if tweet_text else 0,
                }
                
                processed_records.append(processed_record)
                
                # Progress indicator
                if (i + 1) % 100 == 0:
                    print(f"   Processed {i + 1}/{len(self.raw_data)} records...")
                
            except Exception as e:
                print(f"⚠️ Error processing record {i}: {e}")
                skipped_records += 1
                continue
        
        self.processed_df = pd.DataFrame(processed_records)
        
        print(f"✅ Successfully processed {len(self.processed_df)} records")
        if skipped_records > 0:
            print(f"⚠️ Skipped {skipped_records} records due to errors")
        
        return True
    
    def _extract_text(self, record):
        """Extract tweet text from various possible fields"""
        text_fields = ['text', 'full_text', 'content', 'tweet_text', 'message']
        
        for field in text_fields:
            if field in record and record[field]:
                return str(record[field])
        
        return ''
    
    def _extract_user_info(self, record):
        """Extract user information"""
        user_info = {}
        
        # Try to find user object
        user_obj = None
        if 'user' in record and isinstance(record['user'], dict):
            user_obj = record['user']
        elif 'author' in record and isinstance(record['author'], dict):
            user_obj = record['author']
        else:
            user_obj = record  # Sometimes user data is at root level
        
        if user_obj:
            user_info = {
                'username': user_obj.get('screen_name', user_obj.get('username', '')),
                'display_name': user_obj.get('name', user_obj.get('display_name', '')),
                'followers': user_obj.get('followers_count', user_obj.get('followers', 0)),
                'following': user_obj.get('friends_count', user_obj.get('following', 0)),
                'verified': user_obj.get('verified', False),
                'user_id': user_obj.get('id_str', user_obj.get('id', '')),
                'created_at': user_obj.get('created_at', ''),
                'location': user_obj.get('location', ''),
            }
        
        return user_info
    
    def _extract_engagement(self, record):
        """Extract engagement metrics"""
        return {
            'likes': record.get('favorite_count', record.get('favourites_count', record.get('likes', 0))),
            'retweets': record.get('retweet_count', record.get('retweets', 0)),
            'replies': record.get('reply_count', record.get('replies', 0)),
            'quotes': record.get('quote_count', record.get('quotes', 0)),
            'bookmarks': record.get('bookmark_count', record.get('bookmarks', 0)),
        }
    
    def _extract_timing(self, record):
        """Extract and parse timing information"""
        timing = {'created_at': record.get('created_at', ''), 'timestamp': None}
        
        if timing['created_at']:
            try:
                # Try different date formats
                dt = self._parse_twitter_date(timing['created_at'])
                if dt:
                    timing.update({
                        'timestamp': dt,
                        'date': dt.strftime('%Y-%m-%d'),
                        'time': dt.strftime('%H:%M:%S'),
                        'hour': dt.hour,
                        'day_of_week': dt.strftime('%A'),
                    })
            except Exception:
                pass
        
        return timing
    
    def _extract_metadata(self, record):
        """Extract tweet metadata"""
        return {
            'is_retweet': 'retweeted_status' in record or record.get('retweeted', False),
            'is_quote': 'quoted_status' in record or record.get('is_quote_status', False),
            'is_reply': 'in_reply_to_status_id' in record or record.get('in_reply_to_user_id') is not None,
            'is_thread': record.get('is_thread', False),
            'language': record.get('lang', ''),
            'source': record.get('source', ''),
            'possibly_sensitive': record.get('possibly_sensitive', False),
        }
    
    def _extract_entities(self, record):
        """Extract entities like hashtags, mentions, URLs, media"""
        entities = record.get('entities', {})
        extended_entities = record.get('extended_entities', {})
        
        # Extract hashtags
        hashtags = []
        if 'hashtags' in entities:
            hashtags = [f"#{tag['text'].lower()}" for tag in entities['hashtags']]
        
        # Also extract from text
        tweet_text = self._extract_text(record)
        if tweet_text:
            text_hashtags = re.findall(r'#\w+', tweet_text.lower())
            hashtags.extend(text_hashtags)
        
        hashtags = list(set(hashtags))  # Remove duplicates
        
        # Extract mentions
        mentions = []
        if 'user_mentions' in entities:
            mentions = [f"@{mention['screen_name']}" for mention in entities['user_mentions']]
        
        # Extract URLs
        urls = []
        if 'urls' in entities:
            urls = [url['expanded_url'] for url in entities['urls'] if url.get('expanded_url')]
        
        # Extract media
        media = entities.get('media', []) + extended_entities.get('media', [])
        media_types = [m.get('type', '') for m in media]
        
        return {
            'hashtags': hashtags,
            'mentions': mentions,
            'urls': urls,
            'hashtag_count': len(hashtags),
            'mention_count': len(mentions),
            'url_count': len(urls),
            'has_media': len(media) > 0,
            'media_count': len(media),
            'media_types': media_types,
        }
    
    def _parse_twitter_date(self, date_str):
        """Parse Twitter date format to datetime"""
        if not date_str:
            return None
        
        # Try different formats
        formats = [
            "%a %b %d %H:%M:%S %z %Y",  # Twitter format
            "%Y-%m-%dT%H:%M:%S.%fZ",    # ISO format
            "%Y-%m-%d %H:%M:%S",        # Simple format
            "%Y-%m-%dT%H:%M:%SZ",       # ISO without microseconds
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        return None
    
    def _calculate_engagement_rate(self, engagement, followers):
        """Calculate engagement rate"""
        if followers == 0:
            return 0
        
        total_engagement = engagement.get('likes', 0) + engagement.get('retweets', 0) + engagement.get('replies', 0)
        return (total_engagement / followers) * 100 if followers > 0 else 0
    
    def filter_finance_bill_content(self):
        """Filter tweets related to Finance Bill"""
        if self.processed_df is None:
            print("❌ No processed data available. Run process_data() first.")
            return False
        
        print("🔍 Filtering Finance Bill related content...")
        
        # Filter by hashtags
        def has_finance_hashtags(hashtags):
            if not hashtags:
                return False
            return any(hashtag in self.finance_bill_hashtags for hashtag in hashtags)
        
        # Filter by keywords in text
        def has_finance_keywords(text):
            if not text:
                return False
            text_lower = str(text).lower()
            return any(keyword in text_lower for keyword in self.finance_keywords)
        
        # Apply filters
        hashtag_filter = self.processed_df['hashtags'].apply(has_finance_hashtags)
        keyword_filter = self.processed_df['text'].apply(has_finance_keywords)
        
        # Combine filters
        finance_filter = hashtag_filter | keyword_filter
        
        self.finance_bill_df = self.processed_df[finance_filter].copy()
        
        print(f"✅ Found {len(self.finance_bill_df)} Finance Bill related tweets ({len(self.finance_bill_df)/len(self.processed_df)*100:.1f}%)")
        
        return True
    
    def generate_influencer_metrics(self):
        """Generate influencer metrics from Finance Bill content"""
        if self.finance_bill_df is None:
            print("❌ No Finance Bill data available. Run filter_finance_bill_content() first.")
            return False
        
        print("👥 Generating influencer metrics...")
        
        # Group by author and calculate metrics
        influencer_stats = self.finance_bill_df.groupby('author_username').agg({
            'likes': ['sum', 'mean', 'max'],
            'retweets': ['sum', 'mean', 'max'],
            'replies': ['sum', 'mean', 'max'],
            'quotes': ['sum', 'mean', 'max'],
            'total_engagement': ['sum', 'mean', 'max'],
            'tweet_id': 'count',
            'author_followers': 'first',
            'author_following': 'first',
            'author_display_name': 'first',
            'author_verified': 'first',
            'author_created_at': 'first',
            'location': 'first',
            'created_timestamp': ['min', 'max'],
        }).round(2)
        
        # Flatten column names
        influencer_stats.columns = [
            'total_likes', 'avg_likes', 'max_likes',
            'total_retweets', 'avg_retweets', 'max_retweets',
            'total_replies', 'avg_replies', 'max_replies',
            'total_quotes', 'avg_quotes', 'max_quotes',
            'total_engagement', 'avg_engagement', 'max_engagement',
            'tweet_count',
            'author_followers', 'author_following', 'author_display_name',
            'author_verified', 'author_created_at', 'location',
            'first_tweet', 'last_tweet'
        ]
        
        # Calculate additional metrics
        influencer_stats['engagement_rate'] = (
            influencer_stats['total_engagement'] / influencer_stats['author_followers'] * 100
        ).round(2)
        
        influencer_stats['avg_engagement_per_tweet'] = (
            influencer_stats['total_engagement'] / influencer_stats['tweet_count']
        ).round(2)
        
        influencer_stats['likes_per_tweet'] = (
            influencer_stats['total_likes'] / influencer_stats['tweet_count']
        ).round(2)
        
        influencer_stats['retweets_per_tweet'] = (
            influencer_stats['total_retweets'] / influencer_stats['tweet_count']
        ).round(2)
        
        # Calculate activity span
        influencer_stats['first_tweet'] = pd.to_datetime(influencer_stats['first_tweet'], errors='coerce')
        influencer_stats['last_tweet'] = pd.to_datetime(influencer_stats['last_tweet'], errors='coerce')
        influencer_stats.dropna(subset=['first_tweet', 'last_tweet'], inplace=True)

        influencer_stats['activity_span_days'] = (
    influencer_stats['last_tweet'] - influencer_stats['first_tweet']
).dt.days

        
        # Sort by total engagement
        influencer_stats = influencer_stats.sort_values('total_engagement', ascending=False)
        
        # Add ranking
        influencer_stats['rank'] = range(1, len(influencer_stats) + 1)
        
        # Reset index to make username a column
        influencer_stats = influencer_stats.reset_index()
        
        self.influencers_df = influencer_stats
        
        print(f"✅ Generated metrics for {len(self.influencers_df)} influencers")
        
        return True
    
    def save_processed_data(self):
        """Save all processed data to CSV files"""
        if self.processed_df is None:
            print("❌ No processed data to save.")
            return False
        
        print("💾 Saving processed data to CSV files...")
        
        try:
            # Save main processed data
            processed_path = 'data/processed_data/x_processed.csv'
            self.processed_df.to_csv(processed_path, index=False)
            print(f"✅ Saved x_processed.csv ({len(self.processed_df)} records)")
            
            # Save Finance Bill data
            if self.finance_bill_df is not None:
                finance_path = 'data/processed_data/x_finance_bill.csv'
                self.finance_bill_df.to_csv(finance_path, index=False)
                print(f"✅ Saved x_finance_bill.csv ({len(self.finance_bill_df)} records)")
            
            # Save influencer data
            if self.influencers_df is not None:
                influencer_path = 'data/processed_data/finance_bill_influencers.csv'
                self.influencers_df.to_csv(influencer_path, index=False)
                print(f"✅ Saved finance_bill_influencers.csv ({len(self.influencers_df)} records)")
            
            return True
            
        except Exception as e:
            print(f"❌ Error saving data: {e}")
            return False
    
    def generate_summary_report(self):
        """Generate a summary report of the processed data"""
        if self.processed_df is None:
            return
        
        print("\n" + "="*60)
        print("📊 DATA PROCESSING SUMMARY REPORT")
        print("="*60)
        
        # Basic statistics
        print(f"📈 Dataset Overview:")
        print(f"   Total tweets processed: {len(self.processed_df):,}")
        
        if self.finance_bill_df is not None:
            print(f"   Finance Bill related tweets: {len(self.finance_bill_df):,}")
            print(f"   Finance Bill percentage: {len(self.finance_bill_df)/len(self.processed_df)*100:.1f}%")
        
        if self.influencers_df is not None:
            print(f"   Unique influencers identified: {len(self.influencers_df):,}")
        
        # Time range
        if 'created_timestamp' in self.processed_df.columns:
            timestamps = pd.to_datetime(self.processed_df['created_timestamp']).dropna()
            if len(timestamps) > 0:
                print(f"   Date range: {timestamps.min().date()} to {timestamps.max().date()}")
        
        # Engagement summary
        if self.finance_bill_df is not None:
            total_likes = self.finance_bill_df['likes'].sum()
            total_retweets = self.finance_bill_df['retweets'].sum()
            total_replies = self.finance_bill_df['replies'].sum()
            
            print(f"\n🎯 Finance Bill Engagement:")
            print(f"   Total likes: {total_likes:,}")
            print(f"   Total retweets: {total_retweets:,}")
            print(f"   Total replies: {total_replies:,}")
            print(f"   Total engagement: {total_likes + total_retweets + total_replies:,}")
        
        # Top influencers
        if self.influencers_df is not None:
            print(f"\n👑 Top 10 Influencers by Engagement:")
            top_10 = self.influencers_df.head(10)
            for i, row in top_10.iterrows():
                verified = "✅" if row['author_verified'] else ""
                print(f"   {row['rank']:2d}. @{row['author_username']} {verified}")
                print(f"       Tweets: {row['tweet_count']}, Engagement: {row['total_engagement']:,}")
        
        # Hashtag analysis
        if self.finance_bill_df is not None:
            print(f"\n🔥 Most Common Finance Bill Hashtags:")
            all_hashtags = []
            for hashtag_list in self.finance_bill_df['hashtags'].dropna():
                if isinstance(hashtag_list, str):
                    try:
                        hashtag_list = eval(hashtag_list)
                    except:
                        continue
                all_hashtags.extend(hashtag_list)
            
            hashtag_counts = Counter(all_hashtags)
            for hashtag, count in hashtag_counts.most_common(10):
                print(f"   {hashtag}: {count:,} tweets")
        
        print("\n✅ Data processing complete!")
        print("📁 Files saved in: data/processed_data/")
        print("   - x_processed.csv (all processed tweets)")
        print("   - x_finance_bill.csv (Finance Bill related tweets)")
        print("   - finance_bill_influencers.csv (influencer metrics)")
    
    def process_all(self):
        """Run the complete processing pipeline"""
        print("🚀 Starting complete X/Twitter data processing pipeline...")
        
        # Step 1: Load data
        if not self.load_data():
            return False
        
        # Step 2: Process data
        if not self.process_data():
            return False
        
        # Step 3: Filter Finance Bill content
        if not self.filter_finance_bill_content():
            return False
        
        # Step 4: Generate influencer metrics
        if not self.generate_influencer_metrics():
            return False
        
        # Step 5: Save processed data
        if not self.save_processed_data():
            return False
        
        # Step 6: Generate summary report
        self.generate_summary_report()
        
        return True

# Main execution
if __name__ == "__main__":
    # Initialize processor with your JSON file
    processor = XDataProcessor("data/scraped_data/your_x_file.json")
    
    # Run complete processing pipeline
    success = processor.process_all()
    
    if success:
        print("\n🎉 Processing completed successfully!")
        print("Ready to run visualizations!")
    else:
        print("\n❌ Processing failed. Check the error messages above.")