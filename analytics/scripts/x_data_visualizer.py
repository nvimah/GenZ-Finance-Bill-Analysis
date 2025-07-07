"""
X (Twitter) Data Visualization Suite for Gen-Z Finance Bill Analysis
Create comprehensive visualizations for narrative network analysis
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import networkx as nx
from wordcloud import WordCloud
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

class XDataVisualizer:
    def __init__(self, processed_csv_path, finance_bill_csv_path, influencers_csv_path):
        """
        Initialize visualizer with processed data files
        
        Args:
            processed_csv_path: Path to x_processed.csv
            finance_bill_csv_path: Path to x_finance_bill.csv  
            influencers_csv_path: Path to finance_bill_influencers.csv
        """
        self.processed_df = pd.read_csv(processed_csv_path)
        self.finance_df = pd.read_csv(finance_bill_csv_path)
        self.influencers_df = pd.read_csv(influencers_csv_path)
        
        # Convert datetime columns
        self.processed_df['created_timestamp'] = pd.to_datetime(self.processed_df['created_timestamp'])
        self.finance_df['created_timestamp'] = pd.to_datetime(self.finance_df['created_timestamp'])
        
        # Set style for better visualizations
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
        
    def create_timeline_visualization(self):
        """Create comprehensive timeline visualizations"""
        fig = make_subplots(
            rows=4, cols=1,
            subplot_titles=[
                'Daily Tweet Volume - Finance Bill Campaign',
                'Hourly Activity Patterns',
                'Cumulative Engagement Over Time',
                'Tweet Type Distribution Over Time'
            ],
            specs=[[{"secondary_y": False}],
                   [{"secondary_y": False}],
                   [{"secondary_y": True}],
                   [{"secondary_y": False}]]
        )
        
        # Daily volume
        daily_tweets = self.finance_df.groupby(self.finance_df['created_timestamp'].dt.date).size()
        fig.add_trace(
            go.Scatter(x=daily_tweets.index, y=daily_tweets.values, 
                      name='Daily Tweets', line=dict(color='#1f77b4', width=3)),
            row=1, col=1
        )
        
        # Hourly patterns
        hourly_tweets = self.finance_df.groupby(self.finance_df['created_timestamp'].dt.hour).size()
        fig.add_trace(
            go.Bar(x=hourly_tweets.index, y=hourly_tweets.values,
                   name='Hourly Activity', marker_color='#ff7f0e'),
            row=2, col=1
        )
        
        # Cumulative engagement
        daily_engagement = self.finance_df.groupby(self.finance_df['created_timestamp'].dt.date).agg({
            'likes': 'sum',
            'retweets': 'sum',
            'replies': 'sum'
        }).cumsum()
        
        fig.add_trace(
            go.Scatter(x=daily_engagement.index, y=daily_engagement['likes'],
                      name='Cumulative Likes', line=dict(color='#d62728')),
            row=3, col=1
        )
        fig.add_trace(
            go.Scatter(x=daily_engagement.index, y=daily_engagement['retweets'],
                      name='Cumulative Retweets', line=dict(color='#2ca02c')),
            row=3, col=1
        )
        
        # Tweet types over time
        daily_types = self.finance_df.groupby([
            self.finance_df['created_timestamp'].dt.date,
            self.finance_df['is_retweet']
        ]).size().unstack(fill_value=0)
        
        fig.add_trace(
    go.Scatter(
        x=daily_types.index,
        y=daily_types.get(False, pd.Series([0] * len(daily_types.index))),
        name='Original Tweets', stackgroup='one', fill='tonexty'
    ),
    row=4, col=1
)

        fig.add_trace(
         go.Scatter(
          x=daily_types.index,
          y=daily_types.get(True, pd.Series([0] * len(daily_types.index))),
          name='Retweets', stackgroup='one', fill='tonexty'
          ),
    row=4, col=1
)

        
        fig.update_layout(
            height=1200,
            title_text="📅 Timeline Analysis: How Gen-Z Weaponized X/Twitter",
            showlegend=True
        )
        
        return fig
    
    def create_influencer_network(self):
        """Create network visualization of top influencers"""
        # Get top 30 influencers for network analysis
        top_influencers = self.influencers_df.head(30)
        
        # Create network graph
        G = nx.Graph()
        
        # Add nodes (influencers)
        for _, influencer in top_influencers.iterrows():
            G.add_node(
                influencer['author_username'],
                followers=influencer['author_followers'],
                engagement=influencer['total_engagement'],
                tweets=influencer['tweet_count'],
                verified=influencer['author_verified']
            )
        
        # Add edges based on engagement similarity (simplified)
        for i, inf1 in top_influencers.iterrows():
            for j, inf2 in top_influencers.iterrows():
                if i < j:  # Avoid duplicate edges
                    # Create edge if engagement levels are similar
                    eng_diff = abs(inf1['total_engagement'] - inf2['total_engagement'])
                    if eng_diff < inf1['total_engagement'] * 0.5:  # Within 50%
                        G.add_edge(inf1['author_username'], inf2['author_username'])
        
        # Create layout
        pos = nx.spring_layout(G, k=3, iterations=50)
        
        # Extract node information
        node_x = [pos[node][0] for node in G.nodes()]
        node_y = [pos[node][1] for node in G.nodes()]
        node_size = [G.nodes[node]['engagement']/1000 for node in G.nodes()]
        node_color = [G.nodes[node]['followers']/1000 for node in G.nodes()]
        node_text = [f"@{node}<br>Followers: {G.nodes[node]['followers']:,}<br>Engagement: {G.nodes[node]['engagement']:,}" 
                     for node in G.nodes()]
        
        # Create edges
        edge_x = []
        edge_y = []
        for edge in G.edges():
            x0, y0 = pos[edge[0]]
            x1, y1 = pos[edge[1]]
            edge_x.extend([x0, x1, None])
            edge_y.extend([y0, y1, None])
        
        # Create Plotly figure
        fig = go.Figure()
        
        # Add edges
        fig.add_trace(go.Scatter(
            x=edge_x, y=edge_y,
            mode='lines',
            line=dict(width=0.5, color='#888'),
            hoverinfo='none',
            showlegend=False
        ))
        
        # Add nodes
        fig.add_trace(go.Scatter(
            x=node_x, y=node_y,
            mode='markers+text',
            marker=dict(
                size=node_size,
                color=node_color,
                colorscale='Viridis',
                showscale=True,
                colorbar=dict(title="Followers (K)"),
                line=dict(width=2, color='white')
            ),
            text=[node.replace('@', '') for node in G.nodes()],
            textposition="middle center",
            textfont=dict(size=8),
            hoverinfo='text',
            hovertext=node_text,
            showlegend=False
        ))
        
        fig.update_layout(
            title="🕸️ Gen-Z Influencer Network - Finance Bill Campaign",
            showlegend=False,
            hovermode='closest',
            margin=dict(b=20,l=5,r=5,t=40),
            annotations=[ dict(
                text="Node size = Engagement level | Color = Follower count",
                showarrow=False,
                xref="paper", yref="paper",
                x=0.005, y=-0.002,
                font=dict(color="#888", size=12)
            )],
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            width=1000,
            height=800
        )
        
        return fig
    
    def create_engagement_analysis(self):
        """Create detailed engagement analysis visualizations"""
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=[
                'Top 20 Most Engaging Tweets',
                'Engagement Rate by Follower Count',
                'Tweet Type Performance',
                'Verification Status Impact'
            ],
            specs=[[{"type": "xy"}, {"type": "xy"}],
                   [{"type": "xy"}, {"type": "xy"}]]
        )
        
        # Top engaging tweets
        self.finance_df['likes'] = pd.to_numeric(self.finance_df['likes'], errors='coerce')
        self.finance_df['retweets'] = pd.to_numeric(self.finance_df['retweets'], errors='coerce')
        self.finance_df['replies'] = pd.to_numeric(self.finance_df['replies'], errors='coerce') 
        top_tweets = self.finance_df.nlargest(20, 'likes')
        fig.add_trace(
            go.Bar(x=top_tweets['likes'], y=range(len(top_tweets)),
                   orientation='h', name='Likes',
                   text=top_tweets['author_username'],
                   textposition='inside'),
            row=1, col=1
        )
        
        # Engagement rate vs followers
        engagement_rate = (self.finance_df['likes'] + self.finance_df['retweets']) / self.finance_df['author_followers'] * 100
        fig.add_trace(
            go.Scatter(x=self.finance_df['author_followers'], y=engagement_rate,
                      mode='markers', name='Engagement Rate',
                      marker=dict(size=8, opacity=0.6)),
            row=1, col=2
        )
        
        # Tweet type performance
        tweet_performance = self.finance_df.groupby('is_retweet').agg({
            'likes': 'mean',
            'retweets': 'mean',
            'replies': 'mean'
        })
        
        categories = ['Original', 'Retweet']
        fig.add_trace(
            go.Bar(x=categories, y=tweet_performance['likes'],
                   name='Avg Likes', marker_color='#1f77b4'),
            row=2, col=1
        )
        fig.add_trace(
            go.Bar(x=categories, y=tweet_performance['retweets'],
                   name='Avg Retweets', marker_color='#ff7f0e'),
            row=2, col=1
        )
        
        # Verification impact
        verified_performance = self.finance_df.groupby('author_verified').agg({
            'likes': 'mean',
            'retweets': 'mean'
        })
        
        verification_labels = ['Unverified', 'Verified']
        fig.add_trace(
            go.Bar(x=verification_labels, y=verified_performance['likes'],
                   name='Avg Likes (Verified)', marker_color='#2ca02c'),
            row=2, col=2
        )
        
        fig.update_layout(
            height=800,
            title_text="📊 Engagement Analysis: What Made Content Go Viral",
            showlegend=True
        )
        
        return fig
    
    def create_hashtag_evolution(self):
        """Visualize hashtag evolution over time"""
        # Extract hashtags from text
        import re
        
        def extract_hashtags(text):
            if pd.isna(text):
                return []
            return re.findall(r'#\w+', str(text).lower())
        
        self.finance_df['hashtags'] = self.finance_df['text'].apply(extract_hashtags)
        
        # Create daily hashtag counts
        daily_hashtags = {}
        for _, row in self.finance_df.iterrows():
            date = row['created_timestamp'].date()
            for hashtag in row['hashtags']:
                if hashtag not in daily_hashtags:
                    daily_hashtags[hashtag] = {}
                daily_hashtags[hashtag][date] = daily_hashtags[hashtag].get(date, 0) + 1
        
        # Focus on key hashtags
        key_hashtags = ['#rejectfinancebill2024', '#rutomustgo', '#occupyparliament', '#genzkenya']
        
        fig = go.Figure()
        
        for hashtag in key_hashtags:
            if hashtag in daily_hashtags:
                dates = sorted(daily_hashtags[hashtag].keys())
                counts = [daily_hashtags[hashtag][date] for date in dates]
                
                fig.add_trace(go.Scatter(
                    x=dates, y=counts,
                    mode='lines+markers',
                    name=hashtag,
                    line=dict(width=3)
                ))
        
        fig.update_layout(
            title="🔥 Hashtag Evolution: From #RejectFinanceBill to #RutoMustGo",
            xaxis_title="Date",
            yaxis_title="Tweet Count",
            hovermode='x unified',
            width=1000,
            height=600
        )
        
        return fig
    
    def create_wordcloud_analysis(self):
        """Create word clouds for different periods"""
        # Combine all Finance Bill tweet text
        all_text = ' '.join(self.finance_df['text'].dropna().astype(str))
        
        # Remove common words and URLs
        from wordcloud import STOPWORDS
        stopwords = set(STOPWORDS)
        stopwords.update(['https', 'http', 'co', 't', 'amp', 'rt', 'via', 'twitter', 'com'])
        
        # Create word cloud
        wordcloud = WordCloud(
            width=1200, height=600,
            background_color='white',
            stopwords=stopwords,
            max_words=100,
            colormap='viridis'
        ).generate(all_text)
        
        # Create matplotlib figure
        fig, ax = plt.subplots(figsize=(15, 8))
        ax.imshow(wordcloud, interpolation='bilinear')
        ax.axis('off')
        ax.set_title('💬 Word Cloud: Most Common Terms in Finance Bill Tweets', 
                    fontsize=16, fontweight='bold', pad=20)
        
        return fig
    
    def create_influencer_ranking(self):
        """Create comprehensive influencer ranking visualization"""
        top_20 = self.influencers_df.head(20)
        
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=[
                'Top 20 by Total Engagement',
                'Engagement Rate vs Follower Count',
                'Tweet Volume vs Engagement',
                'Verified vs Unverified Performance'
            ]
        )
        
        # Top 20 by engagement
        fig.add_trace(
            go.Bar(x=top_20['total_engagement'], 
                   y=top_20['author_username'],
                   orientation='h',
                   name='Total Engagement',
                   marker_color='#1f77b4'),
            row=1, col=1
        )
        
        # Engagement rate vs followers
        engagement_rate = (top_20['total_engagement'] / top_20['author_followers'] * 100)
        fig.add_trace(
            go.Scatter(x=top_20['author_followers'], y=engagement_rate,
                      mode='markers+text',
                      text=top_20['author_username'],
                      textposition='top center',
                      marker=dict(size=top_20['tweet_count']*2, opacity=0.7),
                      name='Engagement Rate'),
            row=1, col=2
        )
        
        # Tweet volume vs engagement
        fig.add_trace(
            go.Scatter(x=top_20['tweet_count'], y=top_20['total_engagement'],
                      mode='markers+text',
                      text=top_20['author_username'],
                      textposition='top center',
                      marker=dict(size=12, opacity=0.7),
                      name='Volume vs Engagement'),
            row=2, col=1
        )
        
        # Verified vs unverified
        verified_stats = top_20.groupby('author_verified').agg({
            'total_engagement': 'mean',
            'author_followers': 'mean'
        })
        
        fig.add_trace(
            go.Bar(x=['Unverified', 'Verified'], 
                   y=verified_stats['total_engagement'],
                   name='Avg Engagement',
                   marker_color='#ff7f0e'),
            row=2, col=2
        )
        
        fig.update_layout(
            height=800,
            title_text="👑 Influencer Ranking: Who Led the Digital Revolution",
            showlegend=True
        )
        
        return fig
    
    def create_geographic_analysis(self):
        """Analyze geographic distribution of users"""
        # Clean and analyze location data
        location_data = self.finance_df['location'].dropna()
        location_counts = location_data.value_counts().head(20)
        
        fig = go.Figure(data=[
            go.Bar(x=location_counts.values, 
                   y=location_counts.index,
                   orientation='h',
                   marker_color='#2ca02c')
        ])
        
        fig.update_layout(
            title="🌍 Geographic Distribution: Where the Movement Spread",
            xaxis_title="Number of Users",
            yaxis_title="Location",
            height=600,
            width=1000
        )
        
        return fig
    
    def generate_comprehensive_report(self):
        """Generate all visualizations and save them"""
        print("📊 Generating comprehensive visualization report...")
        
        # Create output directory
        import os
        os.makedirs('visualizations', exist_ok=True)
        
        # Generate all visualizations
        visualizations = {
            'timeline': self.create_timeline_visualization(),
            'network': self.create_influencer_network(),
            'engagement': self.create_engagement_analysis(),
            'hashtags': self.create_hashtag_evolution(),
            'wordcloud': self.create_wordcloud_analysis(),
            'influencers': self.create_influencer_ranking(),
            'geographic': self.create_geographic_analysis()
        }
        
        # Save interactive plots
        for name, fig in visualizations.items():
            if hasattr(fig, 'write_html'):  # Plotly figures
                fig.write_html(f'visualizations/{name}_analysis.html')
                print(f"✅ Saved {name}_analysis.html")
            else:  # Matplotlib figures
                fig.savefig(f'visualizations/{name}_analysis.png', dpi=300, bbox_inches='tight')
                print(f"✅ Saved {name}_analysis.png")
        
        # Generate summary statistics
        self.generate_summary_stats()
        
        print("\n🎉 Comprehensive visualization report generated!")
        print("📁 Check the 'visualizations' folder for all files")
        
        return visualizations
    
    def generate_summary_stats(self):
        """Generate key statistics for the report"""
        stats = {
            'total_tweets': len(self.finance_df),
            'total_engagement': self.finance_df[['likes', 'retweets', 'replies']].sum().sum(),
            'top_influencer': self.influencers_df.iloc[0]['author_username'],
            'peak_day': self.finance_df.groupby(self.finance_df['created_timestamp'].dt.date).size().idxmax(),
            'total_influencers': len(self.influencers_df),
            'avg_engagement_per_tweet': self.finance_df['likes'].mean(),
            'date_range': f"{self.finance_df['created_timestamp'].min().date()} to {self.finance_df['created_timestamp'].max().date()}"
        }
        
        # Save to file
        with open('visualizations/summary_stats.txt', 'w') as f:
            f.write("🐦 X/TWITTER FINANCE BILL ANALYSIS - KEY STATISTICS\n")
            f.write("="*60 + "\n\n")
            for key, value in stats.items():
                f.write(f"{key.replace('_', ' ').title()}: {value:,}\n")
        
        print("✅ Summary statistics saved to visualizations/summary_stats.txt")
        return stats

# Usage Example
if __name__ == "__main__":
    # Initialize visualizer with your processed data
    visualizer = XDataVisualizer(
        processed_csv_path="data/processed_data/x_processed.csv",
        finance_bill_csv_path="data/processed_data/x_finance_bill.csv",
        influencers_csv_path="data/processed_data/finance_bill_influencers.csv"
    )
    
    # Generate comprehensive report
    visualizations = visualizer.generate_comprehensive_report()
    
    # Display individual visualizations (optional)
    # visualizations['timeline'].show()
    # visualizations['network'].show()
    # visualizations['engagement'].show()