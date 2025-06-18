# HVAC Climate Data Pipeline - Architecture Diagrams

This directory contains architecture diagrams and documentation for the HVAC Climate Data Pipeline.

## Available Diagrams

- **architecture.mmd**: Complete system architecture using Mermaid format, showing:
  - Overall system architecture
  - Medallion architecture (Bronze → Silver → Gold)
  - Data flow through the pipeline
  - Component interactions

## Viewing Diagrams

The `architecture.mmd` file can be viewed using:
- **GitHub**: Automatically renders Mermaid diagrams
- **VS Code**: Install the Mermaid extension
- **Online**: Use [Mermaid Live Editor](https://mermaid.live/)
- **Documentation**: Include in Markdown files using code blocks

## Architecture Overview

```
┌─────────────────┐
│  Data Sources   │
│  - HVAC Sensors │
│  - Weather APIs │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Bronze Layer   │
│  (Raw Data)     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Silver Layer   │
│  (Cleaned Data) │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Gold Layer     │
│  (Aggregated)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Consumption    │
│  - API          │
│  - Dashboards   │
└─────────────────┘
```
