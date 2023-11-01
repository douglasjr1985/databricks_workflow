import unittest.mock as mock

from main import _get_job_config

# Unit test for the get_job_config function
def test_get_job_config():
    # Given
    with mock.patch('builtins.open', mock.mock_open(read_data='{"key": "value"}')):
        # When
        result = _get_job_config('existing_file')
        # Then
        assert result == {'key': 'value'}