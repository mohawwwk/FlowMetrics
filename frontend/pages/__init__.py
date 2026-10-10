from frontend.pages.about_page import render_about_page
from frontend.pages.business_insights import render_business_insights_page
from frontend.pages.database_explorer import render_database_explorer_page
from frontend.pages.grafana_embed import render_grafana_page
from frontend.pages.home import render_home_page
from frontend.pages.live_analytics import render_live_analytics_page
from frontend.pages.machine_learning import render_machine_learning_page
from frontend.pages.settings_page import render_settings_page
from frontend.pages.system_health import render_system_health_page

__all__ = [
    "render_home_page",
    "render_live_analytics_page",
    "render_machine_learning_page",
    "render_database_explorer_page",
    "render_business_insights_page",
    "render_system_health_page",
    "render_grafana_page",
    "render_settings_page",
    "render_about_page",
]
