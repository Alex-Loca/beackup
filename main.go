package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v2"
)

// Config represents the backup configuration
type Config struct {
	Database struct {
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		Name     string `yaml:"name"`
		User     string `yaml:"user"`
		Password string `yaml:"password"`
	} `yaml:"database"`
	Backup struct {
		OutputDir string        `yaml:"output_dir"`
		Frequency time.Duration `yaml:"frequency"`
		Retention int           `yaml:"retention_days"`
		Format    string        `yaml:"format"` // custom, plain, tar, directory
	} `yaml:"backup"`
	Logging struct {
		Level    string `yaml:"level"`
		FilePath string `yaml:"file_path"`
	} `yaml:"logging"`
}

// BackupTool handles the backup operations
type BackupTool struct {
	config *Config
	logger *log.Logger
}

// NewBackupTool creates a new backup tool instance
func NewBackupTool(configPath string) (*BackupTool, error) {
	config, err := loadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	logger := setupLogger(config)

	return &BackupTool{
		config: config,
		logger: logger,
	}, nil
}

// loadConfig reads and parses the configuration file
func loadConfig(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Set defaults
	if config.Database.Host == "" {
		config.Database.Host = "localhost"
	}
	if config.Database.Port == 0 {
		config.Database.Port = 5432
	}
	if config.Backup.Format == "" {
		config.Backup.Format = "custom"
	}
	if config.Backup.Retention == 0 {
		config.Backup.Retention = 7
	}

	return &config, nil
}

// setupLogger configures logging based on config
func setupLogger(config *Config) *log.Logger {
	var output *os.File = os.Stdout

	if config.Logging.FilePath != "" {
		var err error
		output, err = os.OpenFile(config.Logging.FilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Printf("Failed to open log file, using stdout: %v", err)
			output = os.Stdout
		}
	}

	return log.New(output, "[BACKUP] ", log.LstdFlags|log.Lshortfile)
}

// Start begins the periodic backup process
func (bt *BackupTool) Start() error {
	bt.logger.Println("Starting backup tool...")

	// Ensure output directory exists
	if err := os.MkdirAll(bt.config.Backup.OutputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Run initial backup
	if err := bt.performBackup(); err != nil {
		bt.logger.Printf("Initial backup failed: %v", err)
	}

	// Set up periodic backups
	ticker := time.NewTicker(bt.config.Backup.Frequency)
	defer ticker.Stop()

	for range ticker.C {
		if err := bt.performBackup(); err != nil {
			bt.logger.Printf("Backup failed: %v", err)
		}
	}

	return nil
}

// performBackup executes a single backup operation
func (bt *BackupTool) performBackup() error {
	bt.logger.Println("Starting backup...")

	// Generate backup filename
	timestamp := time.Now().Format("2006-01-02_15-04-05")
	var filename string
	var extension string

	switch bt.config.Backup.Format {
	case "plain":
		extension = ".sql"
	case "tar":
		extension = ".tar"
	case "directory":
		extension = ""
	default: // custom
		extension = ".dump"
	}

	filename = fmt.Sprintf("%s_%s%s", bt.config.Database.Name, timestamp, extension)
	outputPath := filepath.Join(bt.config.Backup.OutputDir, filename)

	// Build pg_dump command
	cmd := bt.buildPgDumpCommand(outputPath)

	// Set environment variables for authentication
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("PGPASSWORD=%s", bt.config.Database.Password),
	)

	bt.logger.Printf("Running: %s", cmd.String())

	// Execute backup
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("pg_dump failed: %w, output: %s", err, string(output))
	}

	bt.logger.Printf("Backup completed successfully: %s", outputPath)

	// Clean up old backups
	if err := bt.cleanupOldBackups(); err != nil {
		bt.logger.Printf("Warning: Failed to cleanup old backups: %v", err)
	}

	return nil
}

// buildPgDumpCommand constructs the pg_dump command with appropriate flags
func (bt *BackupTool) buildPgDumpCommand(outputPath string) *exec.Cmd {
	args := []string{
		"pg_dump",
		"-h", bt.config.Database.Host,
		"-p", fmt.Sprintf("%d", bt.config.Database.Port),
		"-U", bt.config.Database.User,
		"-d", bt.config.Database.Name,
		"--verbose",
		"--no-password",
	}

	// Add format-specific flags
	switch bt.config.Backup.Format {
	case "plain":
		args = append(args, "--format=plain")
	case "tar":
		args = append(args, "--format=tar")
	case "directory":
		args = append(args, "--format=directory")
	default: // custom
		args = append(args, "--format=custom")
	}

	// Add output file/directory
	if bt.config.Backup.Format == "directory" {
		args = append(args, "--file", outputPath)
	} else {
		args = append(args, "--file", outputPath)
	}

	return exec.Command(args[0], args[1:]...)
}

// cleanupOldBackups removes backups older than the retention period
func (bt *BackupTool) cleanupOldBackups() error {
	entries, err := os.ReadDir(bt.config.Backup.OutputDir)
	if err != nil {
		return fmt.Errorf("failed to read backup directory: %w", err)
	}

	cutoff := time.Now().AddDate(0, 0, -bt.config.Backup.Retention)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		if info.ModTime().Before(cutoff) {
			path := filepath.Join(bt.config.Backup.OutputDir, entry.Name())
			if err := os.Remove(path); err != nil {
				bt.logger.Printf("Failed to remove old backup %s: %v", path, err)
			} else {
				bt.logger.Printf("Removed old backup: %s", path)
			}
		}
	}

	return nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: beackup <config-file>")
		os.Exit(1)
	}

	configPath := os.Args[1]

	tool, err := NewBackupTool(configPath)
	if err != nil {
		log.Fatalf("Failed to create backup tool: %v", err)
	}

	// Handle graceful shutdown
	tool.Start()
}
