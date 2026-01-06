"""Generate static HTML page with fight damage timelines."""

import polars as pl
from dotenv import dotenv_values

import fights
from main import IncomingDamage

# Configuration
CONFIG = dotenv_values(".env")
FFLOGS_TOKEN = CONFIG["FFLOGS_TOKEN"]

pl.config.Config.set_tbl_rows(80)

logs = {
    "M9N": {
        "report_id": "HFK9wckdY1jCMZ27",
        "fight_id": 41,
        "party_damage": fights.M9N,
    },
    "M10N": {
        "report_id": "r6VFC3gRb4WzaDmw",
        "fight_id": 65,
        "party_damage": fights.M10N,
    },
    # "M11N": {
    #     "report_id": "vN4py2QhT91XKLHW",
    #     "fight_id": 55,
    #     "party_damage": fights.M11N,
    # },
    # "M12N": {
    #     "report_id": "YCLr1Aj6FJ4yQXNH",
    #     "fight_id": 23,
    #     "party_damage": fights.M12N,
    # },
}


def generate_html_header():
    """Generate HTML header with styling."""
    return """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>FFXIV Incoming Damage Timeline</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <script src="https://cdn.plot.ly/plotly-3.3.0.min.js"></script>
    <style>
        body {
            background-color: #f8f9fa;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
        }
        .navbar {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        }
        .fight-section {
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 2rem;
            padding: 2rem;
        }
        .fight-title {
            color: #667eea;
            font-weight: 600;
            margin-bottom: 1.5rem;
            padding-bottom: 0.5rem;
            border-bottom: 2px solid #667eea;
        }
        .table-container {
            margin-bottom: 2rem;
            overflow-x: auto;
        }
        table {
            font-size: 0.9rem;
        }
        .table thead th {
            background-color: #667eea;
            color: white;
            border: none;
            font-weight: 500;
        }
        .table-striped tbody tr:nth-of-type(odd) {
            background-color: rgba(102, 126, 234, 0.05);
        }
        .plot-container {
            margin-bottom: 2rem;
        }
        .section-subtitle {
            color: #495057;
            font-weight: 500;
            margin-top: 1.5rem;
            margin-bottom: 1rem;
        }
        footer {
            margin-top: 3rem;
            padding: 2rem 0;
            text-align: center;
            color: #6c757d;
        }
    </style>
</head>
<body>
    <nav class="navbar navbar-dark mb-4">
        <div class="container-fluid">
            <span class="navbar-brand mb-0 h1">FFXIV Incoming Damage Timeline</span>
        </div>
    </nav>
    <div class="container">
"""


def generate_html_footer():
    """Generate HTML footer."""
    return """
    </div>
    <footer>
        <p>Generated with FFLogs API data</p>
    </footer>
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
"""


def dataframe_to_html_table(df: pl.DataFrame, table_id: str) -> str:
    """Convert polars DataFrame to HTML table."""
    html = f'<table class="table table-striped table-hover" id="{table_id}">\n'
    html += "  <thead>\n    <tr>\n"

    # Headers
    for col in df.columns:
        html += f"      <th>{col}</th>\n"
    html += "    </tr>\n  </thead>\n"

    # Body
    html += "  <tbody>\n"
    for row in df.iter_rows():
        html += "    <tr>\n"
        for value in row:
            # Handle None/null values
            display_value = "" if value is None else str(value)
            html += f"      <td>{display_value}</td>\n"
        html += "    </tr>\n"
    html += "  </tbody>\n</table>\n"

    return html


def generate_fight_section(fight_name: str, fight_data: dict) -> str:
    """Generate HTML section for a single fight."""
    print(f"Processing {fight_name}...")

    # Create IncomingDamage instance
    incoming_damage = IncomingDamage(
        fight_data["party_damage"],
        report_id=fight_data["report_id"],
        fight_id=fight_data["fight_id"],
        token=FFLOGS_TOKEN,
    )

    # Get party damage data
    party_damage_agg = incoming_damage.get_incoming_damage_profile()
    party_damage_table = party_damage_agg.select(
        "formatted_time",
        "ability_name",
        "unmitigatedAmount",
        "description",
        "damage_type",
    )

    # Debug: Print damage range
    if len(party_damage_agg) > 0:
        min_dmg = party_damage_agg["unmitigatedAmount"].min()
        max_dmg = party_damage_agg["unmitigatedAmount"].max()
        print(f"  Party damage range: {min_dmg:,} - {max_dmg:,}")

    # Generate plots
    party_damage_fig = incoming_damage.plot_party_damage(fight_name)
    tank_damage_fig = incoming_damage.plot_tank_damage(
        fight_name, color_by_target=False
    )

    # Generate HTML
    section_html = f"""
    <div class="fight-section" id="{fight_name.lower()}">
        <h2 class="fight-title">{fight_name}</h2>

        <h3 class="section-subtitle">Party Damage Timeline</h3>
        <div class="table-container">
            {dataframe_to_html_table(party_damage_table, f"{fight_name.lower()}_table")}
        </div>

        <h3 class="section-subtitle">Party Damage Chart</h3>
        <div class="plot-container" id="{fight_name.lower()}_party_plot">
            {party_damage_fig.to_html(include_plotlyjs=False, full_html=False, div_id=f"{fight_name.lower()}_party_div")}
        </div>

        <h3 class="section-subtitle">Tank Damage Chart</h3>
        <div class="plot-container" id="{fight_name.lower()}_tank_plot">
            {tank_damage_fig.to_html(include_plotlyjs=False, full_html=False, div_id=f"{fight_name.lower()}_tank_div")}
        </div>
    </div>
    """

    return section_html


def generate_table_of_contents(fights: dict) -> str:
    """Generate a table of contents navigation."""
    toc_html = """
    <div class="fight-section">
        <h2 class="fight-title">Quick Navigation</h2>
        <div class="list-group">
"""
    for fight_name in fights.keys():
        toc_html += f'            <a href="#{fight_name.lower()}" class="list-group-item list-group-item-action">{fight_name}</a>\n'

    toc_html += """        </div>
    </div>
"""
    return toc_html


def main():
    """Main function to generate the static page."""
    print("Generating static HTML page...")

    # Build HTML content
    html_content = generate_html_header()
    html_content += generate_table_of_contents(logs)

    # Generate section for each fight
    for fight_name, fight_data in logs.items():
        try:
            html_content += generate_fight_section(fight_name, fight_data)
        except Exception as e:
            print(f"Error processing {fight_name}: {e}")
            html_content += f"""
    <div class="fight-section" id="{fight_name.lower()}">
        <h2 class="fight-title">{fight_name}</h2>
        <div class="alert alert-danger" role="alert">
            Error generating data for this fight: {str(e)}
        </div>
    </div>
"""

    html_content += generate_html_footer()

    # Ensure docs directory exists
    import os

    os.makedirs("docs", exist_ok=True)

    # Write to file
    output_file = "docs/index.html"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"Static page generated: {output_file}")


if __name__ == "__main__":
    main()
