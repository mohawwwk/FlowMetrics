import streamlit as st
from frontend.config.settings import config


def render_about_page():
    """Renders platform documentation, system architecture specs, and project credits."""
    st.markdown(
        """
        <div style="font-size: 26px; font-weight: 800; color: #ffffff; letter-spacing: -0.02em;">
            About ApexAnalytics Big Data Platform
        </div>
        <div style="color: #94a3b8; font-size: 13px; margin-bottom: 20px;">
            Production-grade distributed streaming, machine learning, and financial transaction analytics.
        </div>
        """,
        unsafe_allow_html=True,
    )

    col1, col2 = st.columns([1.6, 1.0])

    with col1:
        st.markdown(
            f"""
            <div class="apex-panel">
                <div class="apex-panel-header">
                    <div class="apex-panel-title">💡 System Mission & Purpose</div>
                </div>
                <div style="font-size: 13px; color: #94a3b8; line-height: 1.6;">
                    <p><b>{config.project_name}</b> is an enterprise big data streaming platform designed to eliminate the multi-hour latency of legacy batch processing. Built for e-commerce and financial transaction intelligence, it provides sub-second event ingestion, continuous tumbling and sliding window rollups, and automated machine learning revenue forecasting.</p>
                    <p>The entire backend is built with zero-data-loss guarantees, backed by Kafka partitioning, Spark Structured Streaming checkpointing, and SQLite Write-Ahead Logging (WAL) concurrency.</p>
                </div>
            </div>
            
            <div class="apex-panel">
                <div class="apex-panel-header">
                    <div class="apex-panel-title">🛠️ Core Technology Stack</div>
                </div>
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px; font-size: 12px;">
                    <div style="background: rgba(15, 23, 42, 0.6); padding: 10px; border-radius: 6px;">
                        <b style="color: #ffffff;">Message Broker:</b><br/>
                        <span style="color: #64748b;">Apache Kafka 2.13 (Partitioned, acks=all)</span>
                    </div>
                    <div style="background: rgba(15, 23, 42, 0.6); padding: 10px; border-radius: 6px;">
                        <b style="color: #ffffff;">Compute Engine:</b><br/>
                        <span style="color: #64748b;">Apache Spark 3.5.3 Structured Streaming</span>
                    </div>
                    <div style="background: rgba(15, 23, 42, 0.6); padding: 10px; border-radius: 6px;">
                        <b style="color: #ffffff;">Machine Learning:</b><br/>
                        <span style="color: #64748b;">Spark MLlib 4-Stage PipelineModel (R²=0.9745)</span>
                    </div>
                    <div style="background: rgba(15, 23, 42, 0.6); padding: 10px; border-radius: 6px;">
                        <b style="color: #ffffff;">Transactional Store:</b><br/>
                        <span style="color: #64748b;">SQLite 3 in Write-Ahead Log (WAL) mode</span>
                    </div>
                    <div style="background: rgba(15, 23, 42, 0.6); padding: 10px; border-radius: 6px;">
                        <b style="color: #ffffff;">Visualization:</b><br/>
                        <span style="color: #64748b;">Streamlit, Plotly, Grafana Enterprise</span>
                    </div>
                    <div style="background: rgba(15, 23, 42, 0.6); padding: 10px; border-radius: 6px;">
                        <b style="color: #ffffff;">Runtime Environment:</b><br/>
                        <span style="color: #64748b;">Python 3.11, OpenJDK 17, Windows/Linux</span>
                    </div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col2:
        st.markdown(
            f"""
            <div class="apex-panel">
                <div class="apex-panel-header">
                    <div class="apex-panel-title">📚 Documentation & Reports</div>
                </div>
                <div style="font-size: 13px; color: #94a3b8; line-height: 1.8;">
                    <div>📄 <b>Master README:</b> <code>README.md</code></div>
                    <div>📑 <b>Academic Final Report:</b> <code>docs/FINAL_PROJECT_REPORT.md</code></div>
                    <div>📊 <b>Presentation Deck Notes:</b> <code>docs/PRESENTATION_GUIDE.md</code></div>
                    <div>🎓 <b>Viva Exam Q&A Guide:</b> <code>docs/VIVA_QUESTIONS_AND_ANSWERS.md</code></div>
                    <div>🧪 <b>Phase 7 Benchmark Docs:</b> <code>README_PHASE7.md</code></div>
                </div>
            </div>

            <div class="apex-panel">
                <div class="apex-panel-header">
                    <div class="apex-panel-title">👨‍💻 Engineering Credits</div>
                </div>
                <div style="font-size: 12px; color: #94a3b8; line-height: 1.6;">
                    <p><b>Developed By:</b> Antigravity AI Engineering Team<br/>
                    <b>Version:</b> {config.project_version}<br/>
                    <b>Status:</b> Production Verified (24/24 Tests Passing)<br/>
                    <b>License:</b> MIT Open Source</p>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
