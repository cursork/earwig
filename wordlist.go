package main

import (
	"crypto/rand"
	"fmt"
	"math/big"
)

var adjectives = []string{
	"calm", "bold", "warm", "cool", "fast",
	"slow", "deep", "tall", "wild", "keen",
	"soft", "dark", "grey", "blue", "gold",
	"pale", "rare", "thin", "vast", "wide",
	"late", "fair", "dry", "raw", "old",
	"new", "red", "big", "hot", "icy",
}

var nouns = []string{
	"oak", "fox", "elk", "owl", "moth",
	"fern", "pine", "moss", "reef", "dune",
	"peak", "vale", "cave", "lake", "moon",
	"star", "wind", "rain", "snow", "mist",
	"dawn", "dusk", "tide", "wave", "leaf",
	"root", "bark", "twig", "seed", "stone",
}

func randomCheckpointName() (string, error) {
	ai, err := rand.Int(rand.Reader, big.NewInt(int64(len(adjectives))))
	if err != nil {
		return "", fmt.Errorf("generating random name: %w", err)
	}
	ni, err := rand.Int(rand.Reader, big.NewInt(int64(len(nouns))))
	if err != nil {
		return "", fmt.Errorf("generating random name: %w", err)
	}
	return adjectives[ai.Int64()] + "-" + nouns[ni.Int64()], nil
}
