package utils

import (
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestCmdlineHelpers(t *testing.T) {
	cobra.EnableCommandSorting = false

	// Test normal parsing
	wasRun, rootCmd := makeRunCmd(t)
	rootCmd.SetArgs([]string{"run", "--stringVar", "hello",
		"--stringArr", "a", "--stringArr", "b", "--boolVar", "--intVar", "321"})
	err := rootCmd.Execute()
	assert.NoError(t, err)
	assert.True(t, *wasRun)

	// Test missing args
	wasRun, rootCmd = makeRunCmd(t)
	rootCmd.SetArgs([]string{"run"})
	err = rootCmd.Execute()
	assert.Error(t, err, "required flag `stringVar` has not been set")
	assert.False(t, *wasRun)

	// Test completion
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0660)
	assert.NoError(t, err)
	oldOut := os.Stdout
	defer func() {
		os.Stdout = oldOut
	}()
	os.Stdout = devNull
	wasRun, rootCmd = makeRunCmd(t)
	rootCmd.SetArgs([]string{"completion"})
	err = rootCmd.Execute()
	assert.NoError(t, err)
	assert.False(t, *wasRun)
}

func makeRunCmd(t *testing.T) (*bool, *cobra.Command) {
	wasRun := false

	var rootCmd = &cobra.Command{
		Use: "blobserver",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return CheckRequiredFlags(cmd)
		},
	}
	runCmd := &cobra.Command{
		Use: "run",
		Run: func(cmd *cobra.Command, args []string) {
			// Nope
			assert.Equal(t, "hello", GetFlagS(cmd, "stringVar"))
			assert.EqualValues(t, []string{"a", "b"}, GetFlagSArr(cmd, "stringArr"))
			assert.Equal(t, true, GetFlagB(cmd, "boolVar"))
			assert.Equal(t, int64(321), GetFlagI(cmd, "intVar"))
			wasRun = true
		},
	}
	rootCmd.AddCommand(runCmd)

	runCmd.Flags().String("stringVar", "", "string")
	runCmd.Flags().StringArray("stringArr", []string{}, "string")
	runCmd.Flags().Bool("boolVar", false, "string")
	runCmd.Flags().Int64("intVar", 0, "string")
	_ = runCmd.MarkFlagRequired("stringVar")

	rootCmd.AddCommand(MakeCompletionCmd())

	rootCmd.SilenceErrors = true
	rootCmd.SilenceUsage = true

	return &wasRun, rootCmd
}
