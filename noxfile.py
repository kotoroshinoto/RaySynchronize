"""`noxfile.py` provides session logic for various development needs"""
import nox

@nox.session(name='test')
def test(session):
    """Run the test suite."""
    session.run('pytest')
