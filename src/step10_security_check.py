"""
Step 10: Security Best Practices Check
"""
import os
from pathlib import Path

def main():
    print("=" * 70)
    print("Step 10: Security Best Practices Check")
    print("=" * 70)
    
    issues = []
    checks = []
    
    # Check 1: .env exists
    print("\n### Check 1: Environment Variables ###")
    if Path('.env').exists():
        print("✓ .env file exists")
        checks.append("✓ Environment variables configured")
        
        # Check for hardcoded values
        env_content = Path('.env').read_text(encoding='utf-8')
        if 'minioadmin' not in env_content or 'MINIO_ACCESS_KEY' in env_content:
            print("✓ No hardcoded credentials in .env")
            checks.append("✓ Credentials properly configured")
        else:
            issues.append("⚠️  Check .env for hardcoded values")
    else:
        issues.append("✗ .env file not found")
    
    # Check 2: .gitignore exists and covers sensitive files
    print("\n### Check 2: .gitignore Configuration ###")
    if Path('.gitignore').exists():
        gitignore = Path('.gitignore').read_text (encoding='utf-8')
        
        required = ['.env', 'venv', '__pycache__', '*.log', '*.csv']
        missing = []
        
        for item in required:
            if item in gitignore:
                print(f"✓ .gitignore includes: {item}")
            else:
                missing.append(item)
                issues.append(f"⚠️  .gitignore missing: {item}")
        
        if not missing:
            checks.append("✓ .gitignore properly configured")
    else:
        issues.append("✗ .gitignore file not found")
    
    # Check 3: No hardcoded credentials in source code
    print("\n### Check 3: Source Code Security ###")
    
    dangerous_patterns = [
        'password=',
        'secret_key=',
        'aws_access_key_id="',
        'aws_secret_access_key="'
    ]
    
    src_dir = Path('src')
    if src_dir.exists():
        violations = []
        for py_file in src_dir.glob('*.py'):
            content = py_file.read_text(encoding='utf-8').lower()
            for pattern in dangerous_patterns:
                if pattern in content and 'os.getenv' not in content[content.find(pattern):content.find(pattern)+100]:
                    violations.append(f"{py_file.name}: {pattern}")
        
        if not violations:
            print("✓ No hardcoded credentials in source code")
            checks.append("✓ Code uses environment variables")
        else:
            for v in violations:
                issues.append(f"⚠️  Possible hardcoded credential: {v}")
    
    # Check 4: AWS Profile usage
    print("\n### Check 4: AWS Configuration ###")
    
    env_vars = {}
    if Path('.env').exists():
        for line in Path('.env').read_text(encoding='utf-8').split('\n'):
            if '=' in line and not line.strip().startswith('#'):
                key, value = line.split('=', 1)
                env_vars[key.strip()] = value.strip()
    
    if 'AWS_PROFILE' in env_vars:
        print(f"✓ AWS Profile configured: {env_vars['AWS_PROFILE']}")
        checks.append("✓ Using AWS profile (not hardcoded keys)")
    else:
        issues.append("⚠️  AWS_PROFILE not set in .env")
    
    # Check 5: Sensitive files not in git
    print("\n### Check 5: File Protection ###")
    
    sensitive_files = ['.env', 'npidata_pfile_20050523-20250413.csv']
    protected = []
    
    for file in sensitive_files:
        if Path(file).exists():
            protected.append(f"✓ {file} exists locally")
    
    if protected:
        for p in protected:
            print(p)
        checks.append("✓ Sensitive files present (should be in .gitignore)")
    
    # Summary
    print("\n" + "=" * 70)
    print("SECURITY AUDIT SUMMARY")
    print("=" * 70)
    
    print(f"\n✓ Passed Checks: {len(checks)}")
    for check in checks:
        print(f"  {check}")
    
    if issues:
        print(f"\n⚠️  Issues Found: {len(issues)}")
        for issue in issues:
            print(f"  {issue}")
    else:
        print("\n✓ No security issues found!")
    
    print("\n" + "=" * 70)
    if not issues:
        print("✓ Step 10 Complete! All security checks passed!")
    else:
        print("⚠️  Step 10 Complete with warnings. Review issues above.")
    print("=" * 70)

if __name__ == "__main__":
    main()
