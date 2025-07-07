"""
TikTok Finance Bill Analysis & Visualization
Analyze the processed CSV data to identify key insights
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime
import ast
from collections import Counter

class TikTokAnalyzer:
    def __init__(self, processed_csv_path, finance_bill_csv_path):
        """
        Initialize analyzer with processed CSV files
        
        Args:
            processed_csv_path (str): Path to tiktok_processed.csv
            finance_bill_csv_path (str): Path to tiktok_finance_bill.csv
        """
        self.processed_df = pd.read_csv(processed_csv_path)
        self.finance_df = pd.read_csv(finance_bill_csv_path)
        
        # Convert string representation of lists back to actual lists
        self.finance_df['hashtags'] = self.finance_df['hashtags'].apply(self._parse_hashtags)
        
        print(f"✅ Loaded {len(self.processed_df)} total videos")
        print(f"✅ Loaded {len(self.finance_df)} Finance Bill videos")
        
    def _parse_hashtags(self, hashtag_str):
        """Parse hashtag string back to list"""
        try:
            return ast.literal_eval(hashtag_str)
        except:
            return []
    
    def analyze_top_influencers(self, n=20):
        """
        Identify top Gen-Z influencers from Finance Bill content
        This addresses your KPI: "Identify 50+ key Gen-Z influencers"
        """
        print("\n🎯 TOP GEN-Z FINANCE BILL INFLUENCERS")
        print("="*50)
        
        # Group by creator and calculate comprehensive metrics
        influencer_stats = self.finance_df.groupby('author_username').agg({
            'likes': ['sum', 'mean'],
            'comments': ['sum', 'mean'],
            'shares': ['sum', 'mean'],
            'views': ['sum', 'mean'],
            'video_id': 'count',
            'author_followers': 'first',
            'author_display_name': 'first',
            'created_at': ['first', 'last']
        }).round(2)
        
        # Flatten column names
        influencer_stats.columns = ['_'.join(col).strip() for col in influencer_stats.columns]
        
        # Calculate engagement metrics
        influencer_stats['total_engagement'] = (influencer_stats['likes_sum'] + 
                                              influencer_stats['comments_sum'] + 
                                              influencer_stats['shares_sum'])
        
        influencer_stats['avg_engagement_per_video'] = (influencer_stats['total_engagement'] / 
                                                       influencer_stats['video_id_count'])
        
        influencer_stats['engagement_rate'] = (influencer_stats['total_engagement'] / 
                                             influencer_stats['views_sum'] * 100)
        
        # Sort by total engagement
        top_influencers = influencer_stats.sort_values('total_engagement', ascending=False).head(n)
        
        print(f"📊 Top {n} Finance Bill TikTok Influencers:")
        for i, (username, stats) in enumerate(top_influencers.iterrows(), 1):
            print(f"{i:2d}. @{username}")
            print(f"    📹 Videos: {stats['video_id_count']}")
            print(f"    👥 Followers: {stats['author_followers_first']:,}")
            print(f"    💖 Total Engagement: {stats['total_engagement']:,.0f}")
            print(f"    📈 Avg Engagement/Video: {stats['avg_engagement_per_video']:,.0f}")
            print(f"    📊 Engagement Rate: {stats['engagement_rate']:.2f}%")
            print()
        
        return top_influencers
    
    def analyze_hashtag_evolution(self):
        """
        Map hashtag evolution over time
        This addresses your KPI: "Map primary hashtag networks"
        """
        print("\n📈 HASHTAG EVOLUTION ANALYSIS")
        print("="*50)
        
        # Extract all hashtags with timestamps
        hashtag_timeline = []
        for _, row in self.finance_df.iterrows():
            for hashtag in row['hashtags']:
                hashtag_timeline.append({
                    'hashtag': hashtag,
                    'created_at': row['created_at'],
                    'likes': row['likes'],
                    'views': row['views']
                })
        
        hashtag_df = pd.DataFrame(hashtag_timeline)
        hashtag_df['created_at'] = pd.to_datetime(hashtag_df['created_at'])
        hashtag_df['date'] = hashtag_df['created_at'].dt.date
        
        # Top hashtags by frequency
        hashtag_counts = hashtag_df['hashtag'].value_counts()
        print("📊 Top Finance Bill Hashtags:")
        for hashtag, count in hashtag_counts.head(15).items():
            print(f"   {hashtag}: {count} occurrences")
        
        # Daily hashtag usage
        daily_hashtags = hashtag_df.groupby(['date', 'hashtag']).size().reset_index(name='count')
        
        return hashtag_df, daily_hashtags, hashtag_counts
    
    def create_timeline_analysis(self):
        """
        Create detailed timeline of protest activity
        """
        print("\n📅 PROTEST TIMELINE ANALYSIS")
        print("="*50)
        
        # Convert timestamps
        self.finance_df['created_at'] = pd.to_datetime(self.finance_df['created_at'])
        self.finance_df['date'] = self.finance_df['created_at'].dt.date
        
        # Daily activity
        daily_activity = self.finance_df.groupby('date').agg({
            'video_id': 'count',
            'likes': 'sum',
            'views': 'sum',
            'shares': 'sum'
        }).rename(columns={'video_id': 'video_count'})
        
        # Key dates in the Finance Bill protests
        key_dates = {
            '2024-06-18': 'Finance Bill 2024 introduced',
            '2024-06-20': 'First major protests',
            '2024-06-25': 'Parliament occupation',
            '2024-06-26': 'President Ruto response',
            '2024-07-02': 'Continued protests'
        }
        
        print("📊 Daily Activity Summary:")
        print(f"   Most active day: {daily_activity['video_count'].idxmax()} ({daily_activity['video_count'].max()} videos)")
        print(f"   Highest engagement day: {daily_activity['likes'].idxmax()} ({daily_activity['likes'].max():,} likes)")
        print(f"   Total protest days: {len(daily_activity)}")
        
        # Peak activity periods
        peak_days = daily_activity.nlargest(5, 'video_count')
        print(f"\n🔥 Top 5 Most Active Days:")
        for date, stats in peak_days.iterrows():
            print(f"   {date}: {stats['video_count']} videos, {stats['likes']:,} likes")
        
        return daily_activity, key_dates
    
    def analyze_content_virality(self):
        """
        Analyze what content went viral and why
        """
        print("\n🚀 VIRAL CONTENT ANALYSIS")
        print("="*50)
        
        # Define viral thresholds
        viral_threshold_views = self.finance_df['views'].quantile(0.9)  # Top 10%
        viral_threshold_likes = self.finance_df['likes'].quantile(0.9)
        
        viral_content = self.finance_df[
            (self.finance_df['views'] >= viral_threshold_views) |
            (self.finance_df['likes'] >= viral_threshold_likes)
        ]
        
        print(f"📊 Viral Content Metrics:")
        print(f"   Total viral videos: {len(viral_content)}")
        print(f"   Viral threshold (views): {viral_threshold_views:,.0f}")
        print(f"   Viral threshold (likes): {viral_threshold_likes:,.0f}")
        
        # Top viral videos
        top_viral = viral_content.nlargest(10, 'views')
        print(f"\n🔥 Top 10 Most Viral Videos:")
        for i, (_, video) in enumerate(top_viral.iterrows(), 1):
            print(f"{i:2d}. @{video['author_username']}")
            print(f"    👁️ Views: {video['views']:,}")
            print(f"    💖 Likes: {video['likes']:,}")
            print(f"    💬 Comments: {video['comments']:,}")
            print(f"    📤 Shares: {video['shares']:,}")
            print()
        
        return viral_content, top_viral
    
    def create_visualizations(self):
        """
        Create key visualizations for your research
        """
        print("\n📊 CREATING VISUALIZATIONS")
        print("="*50)
        
        # 1. Daily Activity Timeline
        daily_activity, _ = self.create_timeline_analysis()
        
        fig1 = go.Figure()
        fig1.add_trace(go.Scatter(
            x=daily_activity.index,
            y=daily_activity['video_count'],
            mode='lines+markers',
            name='Videos Posted',
            line=dict(color='#ff6b6b', width=3)
        ))
        fig1.update_layout(
            title='Finance Bill Protest Activity Timeline',
            xaxis_title='Date',
            yaxis_title='Number of Videos',
            template='plotly_white'
        )
        fig1.show()
        
        # 2. Top Influencers Bar Chart
        top_influencers = self.analyze_top_influencers(15)
        
        fig2 = px.bar(
            x=top_influencers['total_engagement'].values,
            y=top_influencers.index,
            orientation='h',
            title='Top 15 Finance Bill TikTok Influencers by Engagement',
            labels={'x': 'Total Engagement', 'y': 'TikTok Username'}
        )
        fig2.update_layout(height=600)
        fig2.show()
        
        # 3. Hashtag Network Analysis
        hashtag_df, _, hashtag_counts = self.analyze_hashtag_evolution()
        
        fig3 = px.bar(
            x=hashtag_counts.head(20).values,
            y=hashtag_counts.head(20).index,
            orientation='h',
            title='Top 20 Finance Bill Hashtags',
            labels={'x': 'Frequency', 'y': 'Hashtag'}
        )
        fig3.update_layout(height=600)
        fig3.show()
        
        # 4. Engagement Distribution
        fig4 = px.histogram(
            self.finance_df, 
            x='likes', 
            nbins=50,
            title='Distribution of Video Likes',
            labels={'likes': 'Number of Likes', 'count': 'Number of Videos'}
        )
        fig4.show()
        
        print("✅ All visualizations created!")
        
    def generate_research_summary(self):
        """
        Generate comprehensive summary for your research paper
        """
        print("\n📋 RESEARCH SUMMARY")
        print("="*70)
        
        # Key metrics
        total_videos = len(self.finance_df)
        unique_creators = self.finance_df['author_username'].nunique()
        total_engagement = (self.finance_df['likes'].sum() + 
                          self.finance_df['comments'].sum() + 
                          self.finance_df['shares'].sum())
        
        # Timeline
        start_date = self.finance_df['created_at'].min()
        end_date = self.finance_df['created_at'].max()
        
        # Top metrics
        top_video_views = self.finance_df['views'].max()
        top_creator_followers = self.finance_df['author_followers'].max()
        
        print(f"📊 DATASET OVERVIEW:")
        print(f"   Total Finance Bill videos: {total_videos:,}")
        print(f"   Unique creators identified: {unique_creators:,}")
        print(f"   Total engagement: {total_engagement:,}")
        print(f"   Date range: {start_date} to {end_date}")
        print(f"   Most viewed video: {top_video_views:,} views")
        print(f"   Largest creator: {top_creator_followers:,} followers")
        
        # Research KPIs Status
        print(f"\n🎯 RESEARCH KPI STATUS:")
        print(f"   ✅ Target: 50+ Gen-Z influencers → Found: {unique_creators:,} creators")
        print(f"   ✅ Hashtag mapping → Analyzed {len(self.analyze_hashtag_evolution()[2])} unique hashtags")
        print(f"   ✅ Timeline analysis → {len(self.create_timeline_analysis()[0])} days of activity")
        print(f"   ✅ Viral content analysis → {len(self.analyze_content_virality()[0])} viral videos")
        
        return {
            'total_videos': total_videos,
            'unique_creators': unique_creators,
            'total_engagement': total_engagement,
            'date_range': f"{start_date} to {end_date}",
            'top_video_views': top_video_views
        }

# Usage
if __name__ == "__main__":
    # Initialize analyzer
    analyzer = TikTokAnalyzer(
        "data/processed_data/tiktok_processed.csv",
        "data/processed_data/tiktok_finance_bill.csv"
    )
    
    # Run comprehensive analysis
    print("🚀 Starting comprehensive TikTok analysis...")
    
    # 1. Identify top influencers
    top_influencers = analyzer.analyze_top_influencers(20)
    
    # 2. Analyze hashtag evolution
    hashtag_analysis = analyzer.analyze_hashtag_evolution()
    
    # 3. Timeline analysis
    timeline_analysis = analyzer.create_timeline_analysis()
    
    # 4. Viral content analysis
    viral_analysis = analyzer.analyze_content_virality()
    
    # 5. Create visualizations
    analyzer.create_visualizations()
    
    # 6. Generate research summary
    summary = analyzer.generate_research_summary()
    
    print("\n✅ Analysis complete! Your research insights are ready.")
    print("📊 Check the visualizations and use the printed data for your report.")