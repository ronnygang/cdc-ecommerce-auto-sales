# Contributing to CDC Ecommerce Auto Sales

Thank you for your interest in contributing! This document provides guidelines for contributing to this project.

## 🤝 How to Contribute

### Reporting Bugs

If you find a bug, please create an issue with:
- Clear description of the problem
- Steps to reproduce
- Expected vs actual behavior
- Environment details (OS, Docker version, etc.)
- Relevant logs

### Suggesting Enhancements

Enhancement suggestions are welcome! Please include:
- Clear description of the feature
- Use cases and benefits
- Potential implementation approach

### Pull Requests

1. **Fork the repository**
2. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Make your changes**
   - Follow existing code style
   - Add tests if applicable
   - Update documentation

4. **Commit your changes**
   ```bash
   git commit -m "feat: add feature description"
   ```

5. **Push to your fork**
   ```bash
   git push origin feature/your-feature-name
   ```

6. **Open a Pull Request**

## 📝 Coding Standards

### Python
- Follow PEP 8 style guide
- Use type hints
- Add docstrings for functions/classes
- Keep functions focused and small

### SQL
- Use uppercase for SQL keywords
- Proper indentation
- Comments for complex queries

### Documentation
- Update README.md if needed
- Add inline comments for complex logic
- Update architecture docs for design changes

## 🧪 Testing

Before submitting a PR:
1. Run health checks: `python scripts/health_check.py`
2. Test with sample data
3. Verify all services start correctly
4. Check for errors in logs

## 📋 Commit Message Convention

Use conventional commits:
- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation changes
- `refactor:` Code refactoring
- `test:` Adding tests
- `chore:` Maintenance tasks

## 🔍 Code Review Process

- All PRs require review
- Address review comments promptly
- Keep PRs focused and reasonably sized
- Update PR description if scope changes

## 📄 License

By contributing, you agree that your contributions will be licensed under the MIT License.

## 💬 Questions?

Feel free to open an issue for any questions about contributing!
