"""
TikTok Data Processor for Gen-Z Finance Bill Analysis
Process Apify TikTok JSON data for analysis
"""

import pandas as pd
import json
import re
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter
import numpy as np

class TikTokDataProcessor:
    def __init__(self, json_file_path):
        """
        Initialize processor with TikTok JSON file from Apify
        
        Args:
            json_file_path (str): Path to your TikTok JSON file
        """
        self.json_file_path = json_file_path
        self.raw_data = None
        self.processed_df = None
        self.finance_bill_hashtags = [
            '#rejectfinancebill2024', '#rutomustgo', '#occupyparliament', 
            '#genzkenya', '#kenyaprotests', '#genzrevolution', '#totalshutdown',
            '#kenyangenz', '#financebill2024', '#youth4change'
        ]
        
    def load_data(self):
        """Load TikTok JSON data from Apify"""
        try:
            with open(self.json_file_path, 'r', encoding='utf-8') as f:
                self.raw_data = json.load(f)
            print(f"✅ Loaded {len(self.raw_data)} TikTok records")
            return True
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            return False
    
    def process_data(self):
        """Process raw TikTok data into structured DataFrame"""
        if not self.raw_data:
            print("❌ No data loaded. Run load_data() first.")
            return None
            
        processed_records = []
        
        for record in self.raw_data:
            try:
                # Extract key fields (adjust based on your JSON structure)
                processed_record = {
                    # Video metadata
                    'video_id': record.get('id', ''),
                    'video_url': record.get('url', ''),
                    'description': record.get('desc', ''),
                    'text': record.get('text', record.get('desc', '')),
                    
                    # Author info
                    'author_username': record.get('author', {}).get('uniqueId', ''),
                    'author_display_name': record.get('author', {}).get('nickname', ''),
                    'author_followers': record.get('author', {}).get('followerCount', 0),
                    'author_verified': record.get('author', {}).get('verified', False),
                    
                    # Engagement metrics
                    'likes': record.get('stats', {}).get('diggCount', 0),
                    'comments': record.get('stats', {}).get('commentCount', 0),
                    'shares': record.get('stats', {}).get('shareCount', 0),
                    'views': record.get('stats', {}).get('playCount', 0),
                    
                    # Timing
                    'created_at': record.get('createTime', ''),
                    'created_timestamp': record.get('createTimeISO', ''),
                    
                    # Music/Sound
                    'music_title': record.get('music', {}).get('title', ''),
                    'music_author': record.get('music', {}).get('authorName', ''),
                    
                    # Video details
                    'duration': record.get('video', {}).get('duration', 0),
                    'video_width': record.get('video', {}).get('width', 0),
                    'video_height': record.get('video', {}).get('height', 0),
                }
                
                processed_records.append(processed_record)
                
            except Exception as e:
                print(f"⚠️ Error processing record: {e}")
                continue
        
        self.processed_df = pd.DataFrame(processed_records)
        print(f"✅ Processed {len(self.processed_df)} records")
        return self.processed_df
    
    def extract_hashtags(self):
        """Extract hashtags from video descriptions"""
        if self.processed_df is None:
            print("❌ No processed data. Run process_data() first.")
            return None
            
        def get_hashtags(text):
            if pd.isna(text):
                return []
            hashtags = re.findall(r'#\w+', str(text).lower())
            return hashtags
        
        self.processed_df['hashtags'] = self.processed_df['text'].apply(get_hashtags)
        self.processed_df['hashtag_count'] = self.processed_df['hashtags'].apply(len)
        
        # Filter for Finance Bill related content
        def has_finance_bill_hashtag(hashtag_list):
            return any(hashtag in self.finance_bill_hashtags for hashtag in hashtag_list)
        
        self.processed_df['finance_bill_related'] = self.processed_df['hashtags'].apply(has_finance_bill_hashtag)
        
        finance_bill_df = self.processed_df[self.processed_df['finance_bill_related'] == True]
        print(f"✅ Found {len(finance_bill_df)} Finance Bill related videos")
        
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
            print(f"   {hashtag}: {count} videos")
        
        return hashtag_counts, finance_bill_counts
    
    def get_top_creators(self, n=20):
        """Get top creators by engagement metrics"""
        if self.processed_df is None:
            return None
            
        # Filter for Finance Bill content
        finance_df = self.processed_df[self.processed_df['finance_bill_related'] == True]
        
        # Group by creator and calculate metrics
        creator_stats = finance_df.groupby('author_username').agg({
            'likes': 'sum',
            'comments': 'sum', 
            'shares': 'sum',
            'views': 'sum',
            'video_id': 'count',
            'author_followers': 'first',
            'author_display_name': 'first'
        }).rename(columns={'video_id': 'video_count'})
        
        # Calculate engagement rate
        creator_stats['total_engagement'] = (creator_stats['likes'] + 
                                           creator_stats['comments'] + 
                                           creator_stats['shares'])
        
        creator_stats['engagement_rate'] = (creator_stats['total_engagement'] / 
                                          creator_stats['views'] * 100)
        
        # Sort by total engagement
        top_creators = creator_stats.sort_values('total_engagement', ascending=False).head(n)
        
        print(f"🎯 Top {n} Finance Bill TikTok Creators:")
        for username, stats in top_creators.iterrows():
            print(f"   @{username}: {stats['video_count']} videos, "
                  f"{stats['total_engagement']:,} total engagement")
        
        return top_creators
    
    def timeline_analysis(self):
        """Analyze posting timeline and activity patterns"""
        if self.processed_df is None:
            return None
            
        # Convert timestamps
        self.processed_df['created_date'] = pd.to_datetime(self.processed_df['created_timestamp'])
        
        # Filter for Finance Bill content
        finance_df = self.processed_df[self.processed_df['finance_bill_related'] == True]
        
        # Daily activity
        daily_activity = finance_df.groupby(finance_df['created_date'].dt.date).size()
        
        print("📅 Timeline Analysis:")
        print(f"   Date range: {daily_activity.index.min()} to {daily_activity.index.max()}")
        print(f"   Peak day: {daily_activity.idxmax()} ({daily_activity.max()} videos)")
        print(f"   Total days active: {len(daily_activity)}")
        
        return daily_activity
    
    def create_summary_report(self):
        """Create comprehensive summary report"""
        if self.processed_df is None:
            print("❌ No data processed yet.")
            return None
            
        print("\n" + "="*50)
        print("📋 TIKTOK FINANCE BILL ANALYSIS SUMMARY")
        print("="*50)
        
        # Basic stats
        total_videos = len(self.processed_df)
        finance_videos = len(self.processed_df[self.processed_df['finance_bill_related'] == True])
        
        print(f"📊 Dataset Overview:")
        print(f"   Total videos: {total_videos:,}")
        print(f"   Finance Bill related: {finance_videos:,} ({finance_videos/total_videos*100:.1f}%)")
        
        # Engagement stats
        finance_df = self.processed_df[self.processed_df['finance_bill_related'] == True]
        if len(finance_df) > 0:
            print(f"\n🎯 Finance Bill Content Engagement:")
            print(f"   Total views: {finance_df['views'].sum():,}")
            print(f"   Total likes: {finance_df['likes'].sum():,}")
            print(f"   Total comments: {finance_df['comments'].sum():,}")
            print(f"   Total shares: {finance_df['shares'].sum():,}")
            print(f"   Average views per video: {finance_df['views'].mean():.0f}")
            print(f"   Average likes per video: {finance_df['likes'].mean():.0f}")
        
        # Top hashtags
        self.analyze_hashtags()
        
        # Top creators
        self.get_top_creators(10)
        
        # Timeline
        self.timeline_analysis()
        
        return {
            'total_videos': total_videos,
            'finance_videos': finance_videos,
            'processed_df': self.processed_df,
            'finance_df': finance_df
        }

# Usage Example
if __name__ == "__main__":
    # Initialize processor
    processor = TikTokDataProcessor("data/scraped_data/your_tiktok_file.json")
    
    # Process data
    if processor.load_data():
        df = processor.process_data()
        finance_df = processor.extract_hashtags()
        summary = processor.create_summary_report()
        
        # Save processed data
        processor.processed_df.to_csv("data/processed_data/tiktok_processed.csv", index=False)
        finance_df.to_csv("data/processed_data/tiktok_finance_bill.csv", index=False)
        
        print("\n✅ Processing complete! Files saved to data/processed_data/")
    else:
        print("❌ Failed to load data. Check your file path.")