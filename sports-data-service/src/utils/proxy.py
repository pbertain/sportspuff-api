"""
Proxy utility for NBA API requests.
Handles Decodo proxy rotation and configuration.
"""

import os
import random
import logging
from typing import Dict, Optional

# Import config - handle both direct import and relative import
try:
    from config import settings
except ImportError:
    try:
        from ..config import settings
    except ImportError:
        from src.config import settings

logger = logging.getLogger(__name__)


class ProxyManager:
    """Manages proxy configuration for NBA API requests."""
    
    def __init__(self):
        # Check environment variables first, then fall back to settings
        # This allows docker-compose environment variables to override
        import os
        self.proxy_enabled = os.getenv('PROXY_ENABLED', '').lower() in ('true', '1', 'yes', 'on') or settings.proxy_enabled
        self.proxy_host = os.getenv('PROXY_HOST', settings.proxy_host)
        self.proxy_username = os.getenv('PROXY_USERNAME', settings.proxy_username)
        self.proxy_password = os.getenv('PROXY_PASSWORD', settings.proxy_password)
        try:
            self.proxy_port_start = int(os.getenv('PROXY_PORT_START', settings.proxy_port_start))
            self.proxy_port_end = int(os.getenv('PROXY_PORT_END', settings.proxy_port_end))
        except (ValueError, TypeError):
            self.proxy_port_start = settings.proxy_port_start
            self.proxy_port_end = settings.proxy_port_end
        self.current_port_index = 0
    
    def get_proxy(self) -> Optional[Dict[str, str]]:
        """
        Get proxy configuration for requests library.
        Returns None if proxy is disabled, otherwise returns proxy dict.
        """
        if not self.proxy_enabled:
            return None
        
        if not self.proxy_username or not self.proxy_password:
            logger.warning("Proxy enabled but credentials not configured")
            return None
        
        # Rotate through available ports
        port = self._get_next_port()
        # Use HTTPS endpoint for HTTPS connections, HTTP for HTTP
        proxy_http = f"http://{self.proxy_username}:{self.proxy_password}@{self.proxy_host}:{port}"
        proxy_https = f"https://{self.proxy_username}:{self.proxy_password}@{self.proxy_host}:{port}"
        
        return {
            'http': proxy_http,
            'https': proxy_https
        }
    
    def _get_next_port(self) -> int:
        """Get next port in rotation (round-robin with some randomness)."""
        # Rotate through ports, but add some randomness to distribute load
        if self.proxy_port_end > self.proxy_port_start:
            port_range = self.proxy_port_end - self.proxy_port_start + 1
            # Mix round-robin with random selection
            if random.random() < 0.3:  # 30% chance to use random port
                port = random.randint(self.proxy_port_start, self.proxy_port_end)
            else:
                port = self.proxy_port_start + (self.current_port_index % port_range)
                self.current_port_index += 1
            return port
        else:
            return self.proxy_port_start
    
    def setup_environment_proxy(self):
        """
        Set HTTP_PROXY and HTTPS_PROXY environment variables.
        This is the most reliable way to make requests library use proxies,
        as it will automatically use them for all requests.
        """
        if not self.proxy_enabled:
            # Clear proxy env vars if disabled
            os.environ.pop('HTTP_PROXY', None)
            os.environ.pop('HTTPS_PROXY', None)
            os.environ.pop('http_proxy', None)
            os.environ.pop('https_proxy', None)
            return
        
        if not self.proxy_username or not self.proxy_password:
            logger.warning("Proxy enabled but credentials not configured")
            return
        
        # Get a proxy URL (we'll use a random port for the initial setup)
        # Note: Environment variables are static, so we'll use a default port
        # For dynamic rotation, we'd need to patch requests.Session
        port = self.proxy_port_start  # Use first port for env var
        proxy_http = f"http://{self.proxy_username}:{self.proxy_password}@{self.proxy_host}:{port}"
        proxy_https = f"https://{self.proxy_username}:{self.proxy_password}@{self.proxy_host}:{port}"
        
        # Set both uppercase and lowercase (requests uses lowercase internally)
        os.environ['HTTP_PROXY'] = proxy_http
        os.environ['HTTPS_PROXY'] = proxy_https
        os.environ['http_proxy'] = proxy_http
        os.environ['https_proxy'] = proxy_https
        
        logger.info(f"Proxy environment variables set: {self.proxy_host}:{port}")
    
    def setup_requests_proxy(self):
        """
        Monkey-patch requests.Session to use proxy with rotation for all requests.
        This provides dynamic port rotation while environment variables are static.
        """
        if not self.proxy_enabled:
            return
        
        try:
            import requests
            
            # Store original request method
            original_request = requests.Session.request
            
            def patched_request(self, method, url, **kwargs):
                # Get fresh proxy for each request (with rotation)
                proxy = proxy_manager.get_proxy()
                if proxy:
                    kwargs['proxies'] = proxy
                return original_request(self, method, url, **kwargs)
            
            # Patch Session.request if not already patched
            if not hasattr(requests.Session.request, '_proxy_patched'):
                requests.Session.request = patched_request
                requests.Session.request._proxy_patched = True
                logger.info("Proxy support enabled for requests library with rotation")
            
        except Exception as e:
            logger.error(f"Failed to setup requests proxy: {e}")


# Global proxy manager instance
proxy_manager = ProxyManager()


def setup_proxy():
    """Setup proxy for NBA API requests using both methods."""
    # Set environment variables (fallback)
    proxy_manager.setup_environment_proxy()
    # Patch requests for dynamic rotation
    proxy_manager.setup_requests_proxy()


def get_proxy_config() -> Optional[Dict[str, str]]:
    """Get current proxy configuration."""
    return proxy_manager.get_proxy()

