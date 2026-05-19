package main

import (
	"log"

	"try-grpc/tutorialpb" // This points to your local folder

	"google.golang.org/protobuf/proto"
)

func main() {
	// Note: You use the folder name 'tutorialpb' to access the structs
	p := &tutorialpb.Person{
		Id:   1234,
		Name: "John Doe",
	}

	out, err := proto.Marshal(p)
	if err != nil {
		log.Fatalln("Failed to encode:", err)
	}

	newP := &tutorialpb.Person{}
	if err := proto.Unmarshal(out, newP); err != nil {
		log.Fatalln("Failed to parse:", err)
	}

	log.Println("Success! Read back name:", newP.GetName())
}
