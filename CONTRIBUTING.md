# Contributing to HVAC Climate Data Pipeline

Thank you for considering contributing to the HVAC Climate Data Pipeline project! This document provides guidelines and instructions for contributing.

## Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/HVAC-Climate-Data-Pipeline.git
   cd HVAC-Climate-Data-Pipeline
   ```
3. **Set up your development environment**:
   ```bash
   ./setup.sh
   # or manually:
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements-dev.txt
   ```

## Development Workflow

### 1. Create a Branch

Create a new branch for your feature or bugfix:

```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/your-bugfix-name
```

### 2. Make Your Changes

- Write clean, readable code
- Follow the existing code style
- Add docstrings to functions and classes
- Update documentation as needed

### 3. Test Your Changes

Run the test suite to ensure your changes don't break existing functionality:

```bash
pytest tests/ -v
```

### 4. Format Your Code

Format your code using Black and isort:

```bash
black .
isort .
```

### 5. Commit Your Changes

Write clear, descriptive commit messages:

```bash
git add .
git commit -m "Add feature: description of your changes"
```

### 6. Push and Create a Pull Request

```bash
git push origin feature/your-feature-name
```

Then create a Pull Request on GitHub.

## Code Style Guidelines

- Follow [PEP 8](https://pep8.org/) style guide
- Use meaningful variable and function names
- Keep functions focused and small
- Add type hints where appropriate
- Write docstrings for all public functions and classes

### Example:

```python
def calculate_average_temperature(
    data: pd.DataFrame, 
    sensor_id: str
) -> float:
    """
    Calculate the average temperature for a specific sensor.
    
    Args:
        data: DataFrame containing sensor data
        sensor_id: Unique identifier for the sensor
        
    Returns:
        Average temperature in Celsius
        
    Raises:
        ValueError: If sensor_id not found in data
    """
    sensor_data = data[data['sensor_id'] == sensor_id]
    if sensor_data.empty:
        raise ValueError(f"Sensor {sensor_id} not found")
    return sensor_data['temperature_celsius'].mean()
```

## Testing Guidelines

- Write tests for all new features
- Ensure existing tests pass
- Aim for high test coverage
- Use pytest fixtures for test data
- Mock external dependencies

### Test Structure:

```python
import pytest

def test_function_name():
    # Arrange
    input_data = create_test_data()
    
    # Act
    result = function_to_test(input_data)
    
    # Assert
    assert result == expected_value
```

## Documentation

- Update README.md if adding new features
- Add docstrings to all functions and classes
- Create or update diagrams if changing architecture

## Project Areas

### High Priority Areas for Contribution

1. **Data Source Integration**
   - Implement real API connectors
   - Add database connectivity
   - Create authentication modules

2. **Transformation Logic**
   - Implement bronze layer transformations
   - Build silver layer business rules
   - Create gold layer aggregations

3. **Data Quality**
   - Add validation rules
   - Implement quality checks
   - Create quality reports

4. **Testing**
   - Add unit tests
   - Add integration tests
   - Add end-to-end tests

5. **Documentation**
   - Create architecture diagrams
   - Write usage guides
   - Add code examples

## Pull Request Process

1. **Description**: Clearly describe what your PR does
2. **Testing**: Include test results
3. **Documentation**: Update relevant documentation
4. **Review**: Address review comments promptly
5. **Merge**: PRs will be merged by maintainers after approval

## Pull Request Template

When creating a PR, include:

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Documentation update
- [ ] Refactoring

## Testing
- [ ] All tests pass
- [ ] New tests added
- [ ] Manual testing completed

## Checklist
- [ ] Code follows project style guidelines
- [ ] Documentation updated
- [ ] Tests added/updated
- [ ] No breaking changes (or documented)
```

## Issue Reporting

When reporting issues:

1. **Check existing issues** first
2. **Use a clear title** describing the problem
3. **Include details**:
   - Steps to reproduce
   - Expected behavior
   - Actual behavior
   - Environment details (OS, Python version, etc.)
   - Error messages and logs

## Code of Conduct

- Be respectful and inclusive
- Welcome newcomers
- Focus on constructive feedback
- Assume good intentions

## Questions?

If you have questions:

1. Check the documentation
2. Search existing issues
3. Open a new issue with the "question" label

## License

By contributing, you agree that your contributions will be licensed under the same license as the project.

---

Thank you for contributing to HVAC Climate Data Pipeline!
