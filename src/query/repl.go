package query

import (
	"bufio"
	"fmt"
	"io"
)

const PROMPT = "BroDB> "
const LOGO = `
________             ________________ 
___  __ )_______________  __ \__  __ )
__  __  |_  ___/  __ \_  / / /_  __  |
_  /_/ /_  /   / /_/ /  /_/ /_  /_/ / 
/_____/ /_/    \____//_____/ /_____/  
                                      
Welcome to BroDB!
`

func Start(in io.Reader, out io.Writer) {
	scanner := bufio.NewScanner(in)
	fmt.Printf("%s \n", LOGO)
	for {
		fmt.Printf(PROMPT)
		scanned := scanner.Scan()
		if !scanned {
			return
		}
		line := scanner.Text()
		if line == "exit" {
			break
		}
		ExecQuery(line)
	}
}
