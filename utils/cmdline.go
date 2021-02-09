package utils

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"os"
)

// CLI tool helpers
func MakeCompletionCmd() *cobra.Command {
	var completionCmd = &cobra.Command{
		Use:   "completion",
		Short: "Generates bash/zsh completion scripts",
		Long: `To load completion run
	. <(completion)
	To configure your bash or zsh shell to load completions for each session add to your
	# ~/.bashrc or ~/.profile
	. <(completion)
	`,
		Run: func(cmd *cobra.Command, args []string) {
			zsh := GetFlagB(cmd, "zsh")
			if zsh {
				_ = cmd.Parent().GenZshCompletion(os.Stdout)
			} else {
				_ = cmd.Parent().GenBashCompletion(os.Stdout)
			}
		},
	}
	completionCmd.Flags().BoolP("zsh", "z", false, "Generate ZSH completion")

	return completionCmd
}

func CheckRequiredFlags(cmd *cobra.Command) error {
	flags := cmd.Flags()

	// No flag checking for completion
	if cmd.Name() == "completion" {
		flags.VisitAll(func(flag *pflag.Flag) {
			_ = flags.SetAnnotation(flag.Name,
				cobra.BashCompOneRequiredFlag, []string{"false"})
		})
		return nil
	}

	requiredError := false
	flagName := ""

	flags.VisitAll(func(flag *pflag.Flag) {
		requiredAnnotation := flag.Annotations[cobra.BashCompOneRequiredFlag]
		if len(requiredAnnotation) == 0 {
			return
		}

		flagRequired := requiredAnnotation[0] == "true"

		if flagRequired && !flag.Changed {
			requiredError = true
			flagName = flag.Name
		}
	})

	if requiredError {
		return fmt.Errorf("required flag `" + flagName + "` has not been set")
	}

	return nil
}

func GetFlagS(cmd *cobra.Command, name string) string {
	val, err := cmd.Flags().GetString(name)
	if err != nil {
		panic(err.Error())
	}
	return val
}

func GetFlagSArr(cmd *cobra.Command, name string) []string {
	val, err := cmd.Flags().GetStringArray(name)
	if err != nil {
		panic(err.Error())
	}
	return val
}

func GetFlagB(cmd *cobra.Command, name string) bool {
	val, err := cmd.Flags().GetBool(name)
	if err != nil {
		panic(err.Error())
	}
	return val
}

func GetFlagI(cmd *cobra.Command, name string) int64 {
	val, err := cmd.Flags().GetInt64(name)
	if err != nil {
		panic(err.Error())
	}
	return val
}
