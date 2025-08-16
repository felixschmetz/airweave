# Datamonkey 🐒🥁

**Backend Integration Testing Framework for Airweave**

A scalable framework for testing connector sync flows end-to-end with real API integrations. The monkey plays the bongo (creates test data) to verify the complete sync pipeline.

## 🚀 Quick Start

### 1. Setup Environment

```bash
# Copy the environment template
cp env.test.example env.test

# Edit with your credentials
nano env.test
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Run GitHub Test

```bash
python run_github_test.py
```

## 🏗️ Architecture

### The Monkey (Test Orchestrator)
- **BaseDatamonkey**: Abstract base class for all connectors
- **Test Flow**: Setup → Create → Update → Delete → Cleanup
- **Performance Tracking**: Sync times, entity counts, processing duration

### The Bongo (Real API Integration)
- **BaseBongo**: Abstract base for connector-specific implementations
- **Real API Calls**: No mocking - tests actual integrations
- **Rate Limiting**: Built-in throttling for API constraints

### Connectors
- **NotionDatamonkey**: Notion-specific implementation
- **Extensible**: Easy to add new connectors

## 📁 Project Structure

```
datamonkey/
├── core/                    # Core framework
│   ├── base_datamonkey.py  # Abstract base class
│   └── test_runner.py      # Test orchestration
       ├── bongos/                 # Real API integrations
       │   ├── base_bongo.py       # Abstract bongo base
       │   └── github_bongo.py     # GitHub API integration
       ├── connectors/              # Connector implementations
       │   └── github_datamonkey.py # GitHub datamonkey
├── utils/                   # Utilities
│   ├── airweave_client.py  # Airweave API client
│   └── logging.py          # Rich logging
       ├── run_github_test.py      # Main test runner
├── requirements.txt         # Dependencies
└── env.test                # Test credentials (gitignored)
```

## 🔧 Configuration

### Environment Variables

```bash
# GitHub Test Credentials
GITHUB_PERSONAL_ACCESS_TOKEN=your_github_pat_here
GITHUB_REPO_NAME=your_username/your_repo_name

# Airweave Backend
AIRWEAVE_API_URL=http://localhost:8001

# Test Configuration
MAX_RETRIES=3
RATE_LIMIT_DELAY_MS=1000
SCORE_THRESHOLD=0.8
```

### GitHub Setup

1. **Create Personal Access Token**: Go to [GitHub Settings > Developer settings > Personal access tokens](https://github.com/settings/tokens)
2. **Generate Token**: Create a new token with `repo` permissions
3. **Repository Access**: Ensure the token has access to your test repository
4. **Repository Name**: Use format `owner/repo` (e.g., `felixschmetz/airweave`)

## 🎯 Test Flow

### Complete Test Cycle

1. **Setup** 🔧
   - Create test collection
   - Configure source connection
   - Initialize sync

       2. **Create & Insert** 🥁
          - Create test files (markdown, Python, JSON) via GitHub API
          - Trigger Airweave sync
          - Verify entities exist in Qdrant with high relevance

       3. **Update** 📝
          - Update test content via GitHub API
          - Re-sync
          - Verify updates in Qdrant

       4. **Delete** 🗑️
          - Remove test files via GitHub API
          - Final sync
          - Verify deletion from Qdrant

5. **Cleanup** 🧹
   - Remove test collection (purges Qdrant data)
   - Verify complete cleanup

## 📊 Performance Metrics

The framework tracks detailed performance metrics:

- **Create Time**: API call duration for entity creation
- **Sync Time**: Airweave sync execution duration
- **Verify Time**: Qdrant verification duration
- **Entity Counts**: Number of entities processed
- **Total Cycle Time**: Complete test cycle duration

## 🔍 Verification Strategy

### Qdrant Validation

- **High Relevance**: Score threshold ≥ 0.8 for entity existence
- **Content Matching**: Verify exact content updates
- **Deletion Confirmation**: Low relevance scores for deleted entities
- **Cleanup Verification**: Collection deletion purges all data

### Search Queries

- **Semantic Search**: Uses entity content for relevance testing
- **Score Thresholds**: Configurable relevance requirements
- **Result Validation**: Comprehensive result verification

## 🚀 Adding New Connectors

### 1. Create Bongo

```python
# datamonkey/bongos/my_connector_bongo.py
from datamonkey.bongos.base_bongo import BaseBongo

class MyConnectorBongo(BaseBongo):
    async def create_entities(self):
        # Implement real API calls
        pass
    
    async def update_entities(self):
        # Implement real API calls
        pass
    
    async def delete_entities(self):
        # Implement real API calls
        pass
    
    async def cleanup(self):
        # Implement cleanup
        pass
```

### 2. Create Datamonkey

```python
# datamonkey/connectors/my_connector_datamonkey.py
from datamonkey.core.base_datamonkey import BaseDatamonkey
from datamonkey.bongos.my_connector_bongo import MyConnectorBongo

class MyConnectorDatamonkey(BaseDatamonkey):
    def _create_bongo(self):
        return MyConnectorBongo(self.test_credentials)
    
    # Implement abstract methods for Airweave integration
    async def _create_test_collection(self):
        pass
    
    # ... other methods
```

### 3. Create Test Runner

```python
# datamonkey/run_my_connector_test.py
from datamonkey.connectors.my_connector_datamonkey import MyConnectorDatamonkey

async def main():
    credentials = {...}
    monkey = MyConnectorDatamonkey("my_connector", credentials)
    await monkey.run_full_test_cycle()
```

## 🐳 Docker Support

### Local Development

```bash
# Run against local Airweave backend
docker-compose up -d  # Start Airweave services
python run_github_test.py  # Run datamonkey
```

### Containerized Testing

```yaml
# docker-compose.test.yml
services:
  datamonkey:
    build: ./datamonkey
    environment:
      - AIRWEAVE_API_URL=http://airweave-backend:8000
    depends_on:
      - airweave-backend
```

## 🔮 Future Enhancements

### Temporal Integration
- **Workflow Orchestration**: Convert test steps to Temporal activities
- **Distributed Testing**: Run tests across multiple environments
- **Scheduled Execution**: Automated daily testing

### Advanced Features
- **Multi-Connector Testing**: Parallel connector validation
- **Performance Benchmarking**: Historical performance tracking
- **Failure Analysis**: Automated root cause analysis
- **CI/CD Integration**: GitHub Actions, GitLab CI

### Monitoring & Alerting
- **Real-time Metrics**: Live test execution monitoring
- **Alert System**: Notifications for test failures
- **Dashboard**: Web-based test result visualization

## 🐛 Troubleshooting

### Common Issues

1. **GitHub API Rate Limits**
   - Framework includes built-in rate limiting
   - Increase `RATE_LIMIT_DELAY_MS` if needed

2. **Airweave Backend Connection**
   - Verify `AIRWEAVE_API_URL` is correct
   - Ensure backend services are running

3. **Authentication Issues**
   - Check GitHub personal access token permissions
   - Verify repository access and branch permissions

4. **Sync Failures**
   - Check Airweave backend logs
   - Verify source connection configuration

### Debug Mode

```bash
# Enable debug logging
export LOG_LEVEL=DEBUG
python run_github_test.py
```

## 📝 Contributing

1. **Fork** the repository
2. **Create** feature branch
3. **Implement** connector or enhancement
4. **Test** thoroughly
5. **Submit** pull request

## 📄 License

MIT License - see LICENSE file for details.

## 🆘 Support

- **Issues**: GitHub Issues
- **Discussions**: GitHub Discussions
- **Documentation**: This README + inline code docs

---

**Happy Testing! 🐒🥁✨**
