import pytest
from unittest.mock import Mock, patch
from lumibot.components.community import CommunityClient


def test_post_uses_scoped_key_and_server_verification_without_retry():
    response=Mock()
    response.json.return_value={'post': {'id': 'post-1'}}
    client=CommunityClient(api_key='test-community-key')
    with patch('lumibot.components.community.requests.post', return_value=response) as post:
        assert client.post('Explain a dated market observation.')['id'] == 'post-1'
        assert post.call_count == 1
        assert post.call_args.kwargs['timeout'] == 15
        assert post.call_args.kwargs['headers']['Authorization'] == 'Bearer test-community-key'
        assert 'sharedTrade' not in post.call_args.kwargs['json']


def test_post_limit_is_checked_without_sending_or_truncating():
    client=CommunityClient(api_key='test-community-key')
    with patch('lumibot.components.community.requests.post') as post:
        with pytest.raises(ValueError): client.post('x'*100001)
        post.assert_not_called()


def test_transport_error_is_visible_and_never_retries():
    client=CommunityClient(api_key='test-community-key')
    with patch('lumibot.components.community.requests.post', side_effect=TimeoutError) as post:
        with pytest.raises(TimeoutError): client.post('Share this observation.')
        assert post.call_count == 1
