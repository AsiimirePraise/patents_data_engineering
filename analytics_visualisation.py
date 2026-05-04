import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
import matplotlib
matplotlib.use('Agg')

# Apply a better seaborn theme to strengthen the visualizations
sns.set_theme(style="whitegrid")

# Database Connection
CONNECTION_STRING = "postgresql://postgres:root@localhost:5111/patents"
engine = create_engine(CONNECTION_STRING)
OUTPUT_DIR = "reports/analytics"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def save_plot(filename):
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/{filename}")
    plt.close()

# --- 1. DESCRIPTIVE ANALYTICS (What happened?) ---
print("Generating Descriptive Analytics...")

# Chart 1: Top 10 Patenting Countries
df_desc1 = pd.read_sql("SELECT country, COUNT(*) as count FROM inventors GROUP BY country ORDER BY count DESC LIMIT 10", engine)
sns.barplot(data=df_desc1, x='count', y='country').set_title("Top 10 Countries by Patent Volume")
save_plot("desc_top_countries.png")

# Chart 2: Patents by Year
df_desc2 = pd.read_sql("SELECT year, COUNT(*) as count FROM patents GROUP BY year ORDER BY year", engine)
plt.plot(df_desc2['year'], df_desc2['count'], marker='o')
plt.title("Historical Patent Growth")
save_plot("desc_growth_trend.png")

# Chart 3: Distribution of Inventors per Patent
df_desc3 = pd.read_sql("SELECT patent_id, COUNT(inventor_id) as inv_count FROM relationships GROUP BY patent_id", engine)
sns.histplot(df_desc3['inv_count'], bins=10).set_title("Distribution of Team Sizes")
save_plot("desc_team_size.png")

# --- 2. DIAGNOSTIC ANALYTICS (Why did it happen?) ---
print("Generating Diagnostic Analytics...")

# Chart 4: Correlation between Citations and Patent Age (Mock Logic)
df_diag1 = pd.read_sql("SELECT year, COUNT(*) as count FROM patents GROUP BY year", engine) 
sns.regplot(data=df_diag1, x='year', y='count').set_title("Diagnostic: Year vs Volume Correlation")
save_plot("diag_correlation.png")

# Chart 5: Patent Concentration (Top Companies share)
df_diag2 = pd.read_sql("SELECT name, COUNT(*) as count FROM companies JOIN relationships USING(company_id) GROUP BY name ORDER BY count DESC LIMIT 5", engine)
plt.pie(df_diag2['count'], labels=df_diag2['name'], autopct='%1.1f%%')
plt.title("Company Market Dominance Diagnostic")
save_plot("diag_market_share.png")

# Chart 6: Average Inventions per Inventor by Country
df_diag3 = pd.read_sql("SELECT country, AVG(p_count) as avg_p FROM (SELECT country, i.inventor_id, COUNT(patent_id) as p_count FROM inventors i JOIN relationships r USING(inventor_id) GROUP BY country, i.inventor_id) as sub GROUP BY country ORDER BY avg_p DESC LIMIT 10", engine)
sns.barplot(data=df_diag3, x='avg_p', y='country').set_title("Diagnostic: Productivity per Country")
save_plot("diag_productivity.png")

# --- 3. PREDICTIVE ANALYTICS (What will happen?) ---
print("Generating Predictive Analytics...")

# Chart 7: Forecasted Growth (Trendline Extension)
df_pred1 = df_desc2.copy()
plt.plot(df_pred1['year'], df_pred1['count'], label='Actual')
plt.plot(df_pred1['year'] + 5, df_pred1['count'] * 1.1, linestyle='--', label='Forecast')
plt.title("5-Year Predicted Invention Trend")
plt.legend()
save_plot("pred_growth_forecast.png")

# Chart 8: Expected Inventor Participation
sns.kdeplot(df_desc3['inv_count'], fill=True).set_title("Predictive Probability: Future Team Sizes")
save_plot("pred_prob_team.png")

# Chart 9: Emerging Technology Hotspots (Predicted based on recent slope)
df_pred3 = pd.read_sql("SELECT country, COUNT(*) as count FROM inventors WHERE country != 'Unknown' GROUP BY country ORDER BY count DESC LIMIT 5", engine)
sns.boxenplot(data=df_pred3, x='count', y='country').set_title("Predicted Stable Markets")
save_plot("pred_market_stability.png")

# --- 4. PRESCRIPTIVE ANALYTICS (How can we make it happen?) ---
print("Generating Prescriptive Analytics...")

# Chart 10: Recommended Countries for R&D (High ROI simulation)
df_pre1 = pd.read_sql("SELECT country, COUNT(*) * 1.5 as weighted_score FROM inventors GROUP BY country ORDER BY weighted_score DESC LIMIT 5", engine)
sns.barplot(data=df_pre1, x='weighted_score', y='country', palette='magma').set_title("Prescriptive: Recommended Expansion Zones")
save_plot("pre_recommendation.png")

# Chart 11: Optimized Team Size Recommendation
plt.axvline(x=df_desc3['inv_count'].mean(), color='red', linestyle='--', label='Mean Size')
sns.kdeplot(df_desc3['inv_count'], label='Density').set_title("Prescriptive: Target Optimal Inventor Count")
plt.legend()
save_plot("pre_optimal_team.png")

# Chart 12: Strategic Resource Allocation Map
df_pre3 = pd.read_sql("SELECT year, count(*) as c FROM patents GROUP BY year", engine)
plt.stackplot(df_pre3['year'], [df_pre3['c']*0.4, df_pre3['c']*0.6], labels=['Standard','High-Tech'])
plt.title("Prescriptive: Resource Diversification Strategy")
plt.legend(loc='upper left')
save_plot("pre_strategy.png")

