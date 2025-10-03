"""
Test script for Snowflake-META Streamlit application
Run this to verify the app loads correctly without errors
"""

import sys
import importlib.util
from pathlib import Path

def test_app_imports():
    """Test that all required imports work"""
    print("Testing imports...")
    
    try:
        import streamlit
        print("✓ streamlit imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import streamlit: {e}")
        return False
    
    try:
        import json
        print("✓ json imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import json: {e}")
        return False
    
    try:
        from datetime import datetime
        print("✓ datetime imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import datetime: {e}")
        return False
    
    return True


def test_app_syntax():
    """Test that app.py has valid syntax"""
    print("\nTesting app.py syntax...")
    
    app_path = Path(__file__).parent / "app.py"
    
    if not app_path.exists():
        print(f"✗ app.py not found at {app_path}")
        return False
    
    try:
        spec = importlib.util.spec_from_file_location("app", app_path)
        if spec is None or spec.loader is None:
            print("✗ Could not load app.py spec")
            return False
            
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        print("✓ app.py syntax is valid")
        return True
    except SyntaxError as e:
        print(f"✗ Syntax error in app.py: {e}")
        return False
    except Exception as e:
        print(f"✗ Error loading app.py: {e}")
        return False


def test_required_files():
    """Test that all required files exist"""
    print("\nTesting required files...")
    
    ui_dir = Path(__file__).parent
    required_files = [
        "app.py",
        "requirements.txt",
        "README.md",
        "QUICKSTART.md",
        ".streamlit/config.toml"
    ]
    
    all_exist = True
    for file_path in required_files:
        full_path = ui_dir / file_path
        if full_path.exists():
            print(f"✓ {file_path} exists")
        else:
            print(f"✗ {file_path} not found")
            all_exist = False
    
    return all_exist


def test_streamlit_config():
    """Test that Streamlit configuration is valid"""
    print("\nTesting Streamlit configuration...")
    
    config_path = Path(__file__).parent / ".streamlit" / "config.toml"
    
    if not config_path.exists():
        print(f"✗ config.toml not found at {config_path}")
        return False
    
    try:
        import toml
        with open(config_path, 'r') as f:
            config = toml.load(f)
        print("✓ config.toml is valid TOML")
        
        if 'theme' in config:
            print("✓ theme section found in config")
        if 'server' in config:
            print("✓ server section found in config")
            
        return True
    except ImportError:
        print("⚠ toml package not installed (optional)")
        return True
    except Exception as e:
        print(f"✗ Error reading config.toml: {e}")
        return False


def main():
    """Run all tests"""
    print("=" * 60)
    print("Snowflake-META Streamlit App Test Suite")
    print("=" * 60)
    
    tests = [
        ("Imports", test_app_imports),
        ("Syntax", test_app_syntax),
        ("Required Files", test_required_files),
        ("Streamlit Config", test_streamlit_config)
    ]
    
    results = {}
    for test_name, test_func in tests:
        results[test_name] = test_func()
    
    print("\n" + "=" * 60)
    print("Test Results Summary")
    print("=" * 60)
    
    for test_name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{test_name:20s}: {status}")
    
    all_passed = all(results.values())
    
    print("=" * 60)
    if all_passed:
        print("✓ All tests passed! The app is ready to run.")
        print("\nTo start the app, run:")
        print("  streamlit run app.py")
        return 0
    else:
        print("✗ Some tests failed. Please fix the issues above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())

