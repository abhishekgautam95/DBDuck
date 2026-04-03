"""
Test that example files run without errors.

This ensures our documentation examples stay working and don't drift from the actual API.
"""
import subprocess
import sys
from pathlib import Path


class TestExampleFiles:
    """Test that example files execute successfully."""
    
    def test_sql_query_builder_example(self):
        """Test the main SQL Query Builder example runs without errors."""
        example_path = Path(__file__).parent.parent / "examples" / "query_builder" / "example_sql_query_builder.py"
        result = subprocess.run(
            [sys.executable, str(example_path)],
            capture_output=True,
            text=True,
            timeout=30
        )
        assert result.returncode == 0, f"Example failed: {result.stderr}"
        assert "All Query Builder examples completed successfully!" in result.stdout
    
    def test_query_builder_imports(self):
        """Test that QueryBuilder can be imported from multiple locations."""
        # Import from DBDuck (recommended)
        from DBDuck import QueryBuilder as QB1
        assert QB1 is not None
        
        # Import from DBDuck.udom (also works)
        from DBDuck.udom import QueryBuilder as QB2
        assert QB2 is not None
        
        # They should be the same class
        assert QB1 is QB2
    
    def test_query_builder_usage_patterns(self):
        """Test both recommended and advanced usage patterns work."""
        from DBDuck import UDOM, QueryBuilder
        
        db = UDOM(url="sqlite:///:memory:")
        db.adapter.run_native("CREATE TABLE test (id INTEGER, name TEXT)")
        db.table("test").create({"id": 1, "name": "Alice"})
        
        # Pattern 1: Via db.table() - recommended
        result1 = db.table("test").find()
        assert len(result1) == 1
        assert result1[0]["name"] == "Alice"
        
        # Pattern 2: Direct instantiation - advanced
        qb = QueryBuilder(db, "test")
        result2 = qb.where(id=1).first()
        assert result2["name"] == "Alice"
