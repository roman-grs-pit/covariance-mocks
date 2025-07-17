"""Unit tests for CLI interface."""

import sys
import argparse
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from io import StringIO

import pytest

from covariance_mocks.cli import (
    list_productions, wrap_with_config_resolution, main
)
from covariance_mocks.production_config import ConfigurationError


class TestListProductions:
    """Test list_productions function."""
    
    @pytest.mark.unit
    @patch('covariance_mocks.cli.get_registry')
    def test_list_productions_success(self, mock_get_registry):
        """Test successful listing of productions."""
        # Mock registry
        mock_registry = Mock()
        mock_registry.list_productions.return_value = {
            'alpha': Path('config/productions/alpha.yaml'),
            'test_basic': Path('config/productions/test_basic.yaml')
        }
        mock_get_registry.return_value = mock_registry
        
        # Capture stdout
        with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
            result = list_productions(None)
        
        assert result == 0
        output = mock_stdout.getvalue()
        assert "Available productions:" in output
        assert "alpha" in output
        assert "test_basic" in output
        assert "config/productions/alpha.yaml" in output
    
    @pytest.mark.unit
    @patch('covariance_mocks.cli.get_registry')
    def test_list_productions_empty(self, mock_get_registry):
        """Test listing with no productions found."""
        mock_registry = Mock()
        mock_registry.list_productions.return_value = {}
        mock_get_registry.return_value = mock_registry
        
        with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
            result = list_productions(None)
        
        assert result == 0
        output = mock_stdout.getvalue()
        assert "No productions found in config/examples/" in output
    
    @pytest.mark.unit
    @patch('covariance_mocks.cli.get_registry')
    def test_list_productions_error(self, mock_get_registry):
        """Test error handling in list_productions."""
        mock_get_registry.side_effect = Exception("Registry error")
        
        with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
            result = list_productions(None)
        
        assert result == 1
        error_output = mock_stderr.getvalue()
        assert "Error listing productions: Registry error" in error_output


class TestConfigResolutionWrapper:
    """Test wrap_with_config_resolution function."""
    
    @pytest.mark.unit
    @patch('covariance_mocks.cli.resolve_config')
    def test_config_resolution_success(self, mock_resolve_config):
        """Test successful config resolution."""
        # Mock resolve_config
        mock_resolve_config.return_value = Path('config/productions/alpha.yaml')
        
        # Mock wrapped function
        mock_func = Mock(return_value=0)
        wrapped_func = wrap_with_config_resolution(mock_func)
        
        # Mock args
        args = Mock()
        args.config = 'alpha'
        
        result = wrapped_func(args)
        
        assert result == 0
        mock_resolve_config.assert_called_once_with('alpha')
        mock_func.assert_called_once_with(args)
        assert args.config == Path('config/productions/alpha.yaml')
    
    @pytest.mark.unit
    @patch('covariance_mocks.cli.resolve_config')
    def test_config_resolution_error(self, mock_resolve_config):
        """Test error handling in config resolution."""
        mock_resolve_config.side_effect = ConfigurationError("Config not found")
        
        mock_func = Mock()
        wrapped_func = wrap_with_config_resolution(mock_func)
        
        args = Mock()
        args.config = 'nonexistent'
        
        with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
            result = wrapped_func(args)
        
        assert result == 1
        error_output = mock_stderr.getvalue()
        assert "Configuration error: Config not found" in error_output
        mock_func.assert_not_called()


class TestMainFunction:
    """Test main CLI function."""
    
    @pytest.mark.unit
    @patch('covariance_mocks.cli.list_productions')
    @patch('sys.argv', ['production-manager', 'list'])
    def test_main_list_command(self, mock_list_productions):
        """Test main function with list command."""
        mock_list_productions.return_value = 0
        
        result = main()
        
        assert result == 0
        mock_list_productions.assert_called_once()
    
    @pytest.mark.unit
    @patch('covariance_mocks.cli.wrap_with_config_resolution')
    @patch('sys.argv', ['production-manager', 'status', 'alpha'])
    def test_main_status_command(self, mock_wrap):
        """Test main function with status command."""
        mock_wrapped_func = Mock(return_value=0)
        mock_wrap.return_value = mock_wrapped_func
        
        result = main()
        
        assert result == 0
        mock_wrapped_func.assert_called_once()
    
    @pytest.mark.unit
    @patch('sys.argv', ['production-manager'])
    def test_main_no_command(self):
        """Test main function with no command (should show help)."""
        with patch('sys.stdout', new_callable=StringIO):
            result = main()
        
        assert result == 1
    
    @pytest.mark.unit
    @patch('sys.argv', ['production-manager', 'init', 'test_config'])
    @patch('covariance_mocks.cli.wrap_with_config_resolution')
    def test_main_init_command(self, mock_wrap):
        """Test main function with init command."""
        mock_wrapped_func = Mock(return_value=0)
        mock_wrap.return_value = mock_wrapped_func
        
        result = main()
        
        assert result == 0
        mock_wrapped_func.assert_called_once()
    
    @pytest.mark.unit
    @patch('sys.argv', ['production-manager', 'monitor', 'alpha', '--interval', '30'])
    @patch('covariance_mocks.cli.wrap_with_config_resolution')
    def test_main_monitor_command_with_interval(self, mock_wrap):
        """Test main function with monitor command and interval."""
        mock_wrapped_func = Mock(return_value=0)
        mock_wrap.return_value = mock_wrapped_func
        
        result = main()
        
        assert result == 0
        mock_wrapped_func.assert_called_once()
        
        # Check that args were parsed correctly
        args = mock_wrapped_func.call_args[0][0]
        assert args.interval == 30
        assert args.config == 'alpha'
    
    @pytest.mark.unit
    @patch('sys.argv', ['production-manager', 'status', 'alpha', '--verbose'])
    @patch('covariance_mocks.cli.wrap_with_config_resolution')
    def test_main_status_verbose(self, mock_wrap):
        """Test main function with status command and verbose flag."""
        mock_wrapped_func = Mock(return_value=0)
        mock_wrap.return_value = mock_wrapped_func
        
        result = main()
        
        assert result == 0
        
        # Check that verbose flag was parsed correctly
        args = mock_wrapped_func.call_args[0][0]
        assert args.verbose is True
    
    @pytest.mark.unit
    @patch('sys.argv', ['production-manager', 'retry', 'alpha', '--submit'])
    @patch('covariance_mocks.cli.wrap_with_config_resolution')
    def test_main_retry_with_submit(self, mock_wrap):
        """Test main function with retry command and submit flag."""
        mock_wrapped_func = Mock(return_value=0)
        mock_wrap.return_value = mock_wrapped_func
        
        result = main()
        
        assert result == 0
        
        # Check that submit flag was parsed correctly
        args = mock_wrapped_func.call_args[0][0]
        assert args.submit is True
    
    @pytest.mark.unit
    @patch('sys.argv', ['production-manager', '--machine', 'local', 'status', 'alpha'])
    @patch('covariance_mocks.cli.wrap_with_config_resolution')
    def test_main_global_machine_option(self, mock_wrap):
        """Test main function with global machine option."""
        mock_wrapped_func = Mock(return_value=0)
        mock_wrap.return_value = mock_wrapped_func
        
        result = main()
        
        assert result == 0
        
        # Check that machine option was parsed correctly
        args = mock_wrapped_func.call_args[0][0]
        assert args.machine == 'local'
    
    @pytest.mark.unit
    @patch('sys.argv', ['production-manager', '--work-dir', '/tmp/test', 'status', 'alpha'])
    @patch('covariance_mocks.cli.wrap_with_config_resolution')
    def test_main_global_work_dir_option(self, mock_wrap):
        """Test main function with global work-dir option."""
        mock_wrapped_func = Mock(return_value=0)
        mock_wrap.return_value = mock_wrapped_func
        
        result = main()
        
        assert result == 0
        
        # Check that work-dir option was parsed correctly
        args = mock_wrapped_func.call_args[0][0]
        assert args.work_dir == Path('/tmp/test')


class TestArgumentParsing:
    """Test argument parsing edge cases."""
    
    @pytest.mark.unit
    def test_parser_help_message(self):
        """Test that help message is properly formatted."""
        with patch('sys.argv', ['production-manager', '--help']):
            with pytest.raises(SystemExit):
                with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                    main()
        
        help_output = mock_stdout.getvalue()
        assert "Production management for covariance mock generation" in help_output
        assert "Examples:" in help_output
        assert "production-manager list" in help_output
    
    @pytest.mark.unit
    def test_subcommand_help(self):
        """Test subcommand help messages."""
        with patch('sys.argv', ['production-manager', 'status', '--help']):
            with pytest.raises(SystemExit):
                with patch('sys.stdout', new_callable=StringIO) as mock_stdout:
                    main()
        
        help_output = mock_stdout.getvalue()
        assert "config" in help_output.lower()
        assert "--verbose" in help_output