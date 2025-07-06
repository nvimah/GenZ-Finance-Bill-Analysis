# GIT SETUP & PROJECT STRUCTURE CREATION
# Run this after you've cloned your repository

import os
import json
from datetime import datetime

def create_project_structure():
    """Create organized folder structure for the project"""
    
    # Check if we're in a git repository
    if not os.path.exists('.git'):
        print("⚠️  Warning: Not in a git repository!")
        print("Make sure you've cloned your GitHub repo first!")
        return
    
    print("🚀 Setting up project structure in Git repository...")
    
    # Create directory structure
    directories = [
        'data/raw_data',
        'data/processed_data',
        'data/scraped_data',
        'analytics/notebooks',
        'analytics/scripts',
        'visualizations/interactive',
        'visualizations/static',
        'reports/drafts',
        'reports/final',
        'documentation',
        'src',  # Source code
        'tests',  # For any testing
        'assets/images',
        'assets/datasets'
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        
        # Create .gitkeep files in empty directories so Git tracks them
        gitkeep_path = os.path.join(directory, '.gitkeep')
        with open(gitkeep_path, 'w') as f:
            f.write('')
        
        print(f"✓ Created directory: {directory}")
    
    print("\n📁 Project structure created successfully!")

def create_gitignore():
    """Create a comprehensive .gitignore file"""
    
    gitignore_content = """# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
pip-wheel-metadata/
share/python-wheels/
*.egg-info/
.installed.cfg
*.egg
MANIFEST

# PyInstaller
*.manifest
*.spec

# Jupyter Notebook
.ipynb_checkpoints

# Environment variables
.env
.venv
env/
venv/
ENV/
env.bak/
venv.bak/

# API Keys and Sensitive Data
config.py
secrets.json
api_keys.txt
*.key
*.pem

# Data files that might be too large for Git
*.csv
*.json
*.xlsx
*.parquet
data/raw_data/*.json
data/scraped_data/*.json
data/processed_data/*.pkl

# Visualization outputs
*.png
*.jpg
*.jpeg
*.gif
*.pdf
visualizations/static/*.png
visualizations/static/*.pdf

# OS generated files
.DS_Store
.DS_Store?
._*
.Spotlight-V100
.Trashes
ehthumbs.db
Thumbs.db

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# Logs
*.log
logs/
"""
    
    with open('.gitignore', 'w') as f:
        f.write(gitignore_content)
    
    print("✓ Enhanced .gitignore created")

def create_readme():
    """Create a professional README.md file"""
    
    readme_content = """# Gen-Z Finance Bill Analysis: Digital Democracy in Action

## 🎯 Project Overview
Advanced digital narrative analysis of Kenya's 2024 Gen-Z led Finance Bill protests - examining how digital platforms enabled unprecedented youth political mobilization.

## 📊 Research Scope
- **Platforms Analyzed**: TikTok, Twitter/X, Instagram
- **Timeline**: June-July 2024
- **Data Points**: 10,000+ posts, 500+ influencers
- **Methods**: NLP, Social Network Analysis, Sentiment Analysis, Machine Learning

## 🔬 Methodology
- **Data Collection**: Web scraping, API integration
- **Analysis**: Network analysis, topic modeling, sentiment tracking
- **Visualization**: Interactive dashboards, network graphs
- **Tools**: Python, NetworkX, Plotly, Scikit-learn

## 📁 Project Structure
```
├── data/
│   ├── raw_data/          # Original scraped data
│   ├── processed_data/    # Cleaned and processed datasets
│   └── scraped_data/      # Platform-specific collections
├── analytics/
│   ├── notebooks/         # Jupyter notebooks for analysis
│   └── scripts/          # Python analysis scripts
├── visualizations/
│   ├── interactive/      # Dashboard and interactive plots
│   └── static/          # Static charts and graphs
├── reports/
│   ├── drafts/          # Work-in-progress reports
│   └── final/           # Final deliverables
├── documentation/        # Technical documentation
├── src/                 # Source code modules
└── assets/             # Images, datasets, resources
```

## 🚀 Key Findings
*[To be populated during analysis]*

## 🛠️ Technologies Used
- **Python**: Data processing and analysis
- **Pandas/NumPy**: Data manipulation
- **NetworkX**: Social network analysis
- **Plotly/Dash**: Interactive visualizations
- **Scikit-learn**: Machine learning
- **NLTK/TextBlob**: Natural language processing

## 📈 Deliverables
- [ ] Interactive dashboard
- [ ] Comprehensive research report (50+ pages)
- [ ] Influencer network analysis
- [ ] Sentiment evolution tracking
- [ ] Methodology white paper
- [ ] Presentation materials

## 🎯 Research Questions
1. How did Gen-Z use digital platforms to organize political action?
2. What narrative strategies proved most effective for mobilization?
3. How did sentiment evolve throughout the protest period?
4. What can CSOs learn from this digital organizing success?

## 📊 Current Status
- [x] Project setup and infrastructure
- [x] Data collection framework
- [ ] Influencer identification and mapping
- [ ] Content scraping and processing
- [ ] Sentiment analysis implementation
- [ ] Network analysis
- [ ] Report writing and visualization

## 🤝 Applications
This research provides actionable insights for:
- Civil society organizations
- Political campaign strategists
- Social media researchers
- Democratic engagement initiatives

## 📧 Contact
*[Your contact information]*

---
*This project demonstrates advanced digital research methodologies for understanding youth political engagement in the digital age.*
"""
    
    with open('README.md', 'w', encoding="utf-8") as f:
        f.write(readme_content)
    
    print("✓ Professional README.md created")

def create_requirements_txt():
    """Create requirements.txt for Python dependencies"""
    
    requirements = """# Core Data Science
pandas>=1.3.0
numpy>=1.21.0
scikit-learn>=1.0.0
matplotlib>=3.4.0
seaborn>=0.11.0
jupyter>=1.0.0

# Network Analysis
networkx>=2.6.0
plotly>=5.0.0
dash>=2.0.0

# Natural Language Processing
textblob>=0.17.0
vaderSentiment>=3.3.0
nltk>=3.6.0

# Web Scraping
beautifulsoup4>=4.9.0
selenium>=4.0.0
requests>=2.25.0

# Social Media APIs
tweepy>=4.0.0
python-twitter>=3.5.0

# Visualization
wordcloud>=1.8.0
plotly-dash>=2.0.0

# Data Storage
openpyxl>=3.0.0
xlsxwriter>=3.0.0

# Machine Learning
scipy>=1.7.0
statsmodels>=0.13.0
"""
    
    with open('requirements.txt', 'w') as f:
        f.write(requirements)
    
    print("✓ requirements.txt created")

def create_initial_commit():
    """Create initial project files and commit"""
    
    # Create project metadata
    project_info = {
        "project_name": "GenZ Finance Bill Analysis",
        "created_date": datetime.now().isoformat(),
        "description": "Digital narrative analysis of Kenya's 2024 Gen-Z protests",
        "status": "Day 1 - Project Setup Complete",
        "target_completion": "8 days",
        "version": "1.0.0"
    }
    
    with open('project_info.json', 'w') as f:
        json.dump(project_info, f, indent=2)
    
    print("✓ Project metadata created")
    
    # Instructions for Git commit
    print("\n🔄 Ready for initial commit!")
    print("Run these commands in your terminal:")
    print("git add .")
    print("git commit -m 'Initial project setup - Day 1 complete'")
    print("git push origin main")

if __name__ == "__main__":
    print("🚀 Setting up Git-based project structure...")
    print("=" * 50)
    
    # Create everything
    create_project_structure()
    create_gitignore()
    create_readme()
    create_requirements_txt()
    create_initial_commit()
    
    print("\n✅ Git setup complete!")
    print("\n📋 What you have now:")
    print("  • Professional folder structure")
    print("  • Comprehensive .gitignore")
    print("  • Professional README.md")
    print("  • requirements.txt for dependencies")
    print("  • Project metadata")
    print("  • Ready for version control")
    
    print("\n🎯 Next steps:")
    print("  1. Run: pip install -r requirements.txt")
    print("  2. Run: git add .")
    print("  3. Run: git commit -m 'Initial project setup'")
    print("  4. Run: git push origin main")
    print("  5. Start Day 1 data collection!")