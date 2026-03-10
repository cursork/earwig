package main

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/nk/earwig/internal/ignore"
	"github.com/nk/earwig/internal/store"
)

// Diff modes
type diffMode int

const (
	vsFilesystem diffMode = iota
	vsParent
)

// Pane focus
type pane int

const (
	topPane pane = iota
	bottomPane
)

// Styles
var (
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("15")).
			Background(lipgloss.Color("62")).
			Padding(0, 1)

	selectedStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("15")).
			Background(lipgloss.Color("236"))

	hereStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("214")).
			Bold(true)

	dimStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("245"))

	statusStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("252")).
			Background(lipgloss.Color("236"))

	separatorStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("245"))

	errorStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("196"))

	searchLabelStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("214")).
				Bold(true)
)

type tuiModel struct {
	// Dependencies
	store  *store.Store
	root   string
	ig     *ignore.Matcher
	headID *int64

	// Snapshot list
	snapshots   []store.Snapshot // newest-first
	checkpoints map[int64][]string // snapshot ID -> checkpoint names
	cursor      int
	listOffset  int

	// Diff state
	diffContent string
	diffLoading bool
	diffErr     error
	diffMode    diffMode
	diffSnap    *store.Snapshot // which snapshot the diff is for

	// Search/filter
	searchActive bool
	searchInput  textinput.Model
	filterQuery  string
	filtered     []int // nil = show all; indices into snapshots

	// Layout
	focus        pane
	width        int
	height       int
	topHeight    int
	diffViewport viewport.Model
}

// Messages
type diffComputedMsg struct {
	content string
	err     error
	snapID  int64 // to discard stale results
}

func cmdTUI(args []string) error {
	s, root, err := openStore()
	if err != nil {
		return err
	}
	defer s.Close()

	ig, err := loadIgnore(root)
	if err != nil {
		return err
	}

	headID, err := readHead(root, s)
	if err != nil {
		return err
	}

	snapshots, err := s.ListSnapshots()
	if err != nil {
		return err
	}
	if len(snapshots) == 0 {
		fmt.Println("No snapshots yet.")
		return nil
	}

	// Load checkpoint names
	cpMap, err := s.CheckpointsBySnapshot()
	if err != nil {
		return err
	}

	// Reverse to newest-first
	for i, j := 0, len(snapshots)-1; i < j; i, j = i+1, j-1 {
		snapshots[i], snapshots[j] = snapshots[j], snapshots[i]
	}

	// Default cursor to HEAD
	cursor := 0
	for i, snap := range snapshots {
		if headID != nil && snap.ID == *headID {
			cursor = i
			break
		}
	}

	ti := textinput.New()
	ti.Placeholder = "filename..."
	ti.CharLimit = 256

	vp := viewport.New(0, 0)

	m := tuiModel{
		store:        s,
		root:         root,
		ig:           ig,
		headID:       headID,
		snapshots:    snapshots,
		checkpoints:  cpMap,
		cursor:       cursor,
		diffViewport: vp,
		searchInput:  ti,
	}

	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err = p.Run()
	return err
}

func (m tuiModel) Init() tea.Cmd {
	return nil
}

func (m tuiModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.updateLayout()
		// Auto-compute diff for initially selected snapshot
		if m.diffSnap == nil && m.listLen() > 0 {
			return m, m.computeDiff()
		}
		return m, nil

	case tea.KeyMsg:
		if m.searchActive {
			return m.updateSearch(msg)
		}
		return m.updateNormal(msg)

	case diffComputedMsg:
		// Discard stale result
		snap := m.selectedSnapshot()
		if snap == nil || snap.ID != msg.snapID {
			return m, nil
		}
		m.diffLoading = false
		m.diffErr = msg.err
		if msg.err != nil {
			m.diffContent = errorStyle.Render("Error: " + msg.err.Error())
		} else {
			m.diffContent = msg.content
		}
		m.diffViewport.SetContent(m.diffContent)
		m.diffViewport.GotoTop()
		return m, nil
	}
	return m, nil
}

func (m *tuiModel) updateLayout() {
	available := m.height - 2 // status bar + separator

	topHeight := available * 40 / 100
	if topHeight < 3 {
		topHeight = 3
	}
	bottomHeight := available - topHeight
	if bottomHeight < 3 {
		bottomHeight = 3
		topHeight = available - bottomHeight
	}

	m.topHeight = topHeight
	m.diffViewport.Width = m.width
	m.diffViewport.Height = bottomHeight
	if m.diffContent != "" {
		m.diffViewport.SetContent(m.diffContent)
	}
}

func (m tuiModel) updateNormal(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	key := msg.String()

	// When bottom pane focused, forward navigation to viewport
	if m.focus == bottomPane {
		switch key {
		case "q", "ctrl+c":
			return m, tea.Quit
		case "tab":
			m.focus = topPane
			return m, nil
		case "t":
			return m.toggleDiffMode()
		case "/":
			m.searchActive = true
			m.searchInput.Focus()
			return m, textinput.Blink
		case "esc":
			if m.filterQuery != "" {
				m.filtered = nil
				m.filterQuery = ""
				m.cursor = 0
				m.listOffset = 0
				return m, m.computeDiff()
			}
			m.focus = topPane
			return m, nil
		}
		// Forward everything else to viewport
		var cmd tea.Cmd
		m.diffViewport, cmd = m.diffViewport.Update(msg)
		return m, cmd
	}

	// Top pane focused
	switch key {
	case "q", "ctrl+c":
		return m, tea.Quit
	case "esc":
		if m.filterQuery != "" {
			m.filtered = nil
			m.filterQuery = ""
			m.cursor = 0
			m.listOffset = 0
			return m, m.computeDiff()
		}
		return m, nil
	case "j", "down":
		if m.cursor < m.listLen()-1 {
			m.cursor++
			m.ensureCursorVisible()
			return m, m.computeDiff()
		}
		return m, nil
	case "k", "up":
		if m.cursor > 0 {
			m.cursor--
			m.ensureCursorVisible()
			return m, m.computeDiff()
		}
		return m, nil
	case "g", "home":
		m.cursor = 0
		m.ensureCursorVisible()
		return m, m.computeDiff()
	case "G", "end":
		m.cursor = m.listLen() - 1
		m.ensureCursorVisible()
		return m, m.computeDiff()
	case "enter", "tab":
		m.focus = bottomPane
		return m, nil
	case "t":
		return m.toggleDiffMode()
	case "/":
		m.searchActive = true
		m.searchInput.Focus()
		return m, textinput.Blink
	}
	return m, nil
}

func (m tuiModel) updateSearch(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "enter":
		query := m.searchInput.Value()
		if query == "" {
			// Empty search clears filter
			m.filtered = nil
			m.filterQuery = ""
		} else {
			m.filterQuery = query
			m.filtered = m.filterSnapshots(query)
		}
		m.searchActive = false
		m.searchInput.Blur()
		m.searchInput.SetValue("")
		m.cursor = 0
		m.listOffset = 0
		if m.listLen() > 0 {
			return m, m.computeDiff()
		}
		m.diffContent = ""
		m.diffViewport.SetContent("")
		return m, nil
	case "esc":
		if m.filterQuery != "" {
			// Clear filter
			m.filtered = nil
			m.filterQuery = ""
			m.cursor = 0
			m.listOffset = 0
			m.searchActive = false
			m.searchInput.Blur()
			m.searchInput.SetValue("")
			return m, m.computeDiff()
		}
		m.searchActive = false
		m.searchInput.Blur()
		m.searchInput.SetValue("")
		return m, nil
	}
	var cmd tea.Cmd
	m.searchInput, cmd = m.searchInput.Update(msg)
	return m, cmd
}

func (m tuiModel) toggleDiffMode() (tuiModel, tea.Cmd) {
	if m.diffMode == vsFilesystem {
		m.diffMode = vsParent
	} else {
		m.diffMode = vsFilesystem
	}
	if m.listLen() > 0 {
		return m, m.computeDiff()
	}
	return m, nil
}

// computeDiff returns a tea.Cmd that computes the diff for the selected snapshot.
// Caller must set m.diffLoading = true before returning from Update.
func (m tuiModel) computeDiff() tea.Cmd {
	snap := m.selectedSnapshot()
	if snap == nil {
		return nil
	}
	s := m.store
	root := m.root
	ig := m.ig
	mode := m.diffMode
	snapCopy := *snap

	return func() tea.Msg {
		var content string
		var err error
		if mode == vsFilesystem {
			content, err = formatRestoreDiffStr(s, root, ig, &snapCopy)
		} else {
			content, err = formatParentDiffStr(s, &snapCopy)
		}
		return diffComputedMsg{content: content, err: err, snapID: snapCopy.ID}
	}
}

func (m tuiModel) selectedSnapshot() *store.Snapshot {
	if m.listLen() == 0 {
		return nil
	}
	idx := m.realIndex(m.cursor)
	return &m.snapshots[idx]
}

// listLen returns the number of visible snapshots (filtered or all).
func (m tuiModel) listLen() int {
	if m.filtered != nil {
		return len(m.filtered)
	}
	return len(m.snapshots)
}

// realIndex maps a cursor position to the actual snapshots slice index.
func (m tuiModel) realIndex(i int) int {
	if m.filtered != nil {
		return m.filtered[i]
	}
	return i
}

func (m *tuiModel) ensureCursorVisible() {
	if m.cursor < m.listOffset {
		m.listOffset = m.cursor
	}
	if m.cursor >= m.listOffset+m.topHeight {
		m.listOffset = m.cursor - m.topHeight + 1
	}
}

func (m tuiModel) filterSnapshots(query string) []int {
	query = filepath.ToSlash(query)
	var indices []int
	for i := range m.snapshots {
		if snapshotTouchesFile(m.store, &m.snapshots[i], query) {
			indices = append(indices, i)
		}
	}
	return indices
}

// View renders the full TUI.
func (m tuiModel) View() string {
	if m.width == 0 {
		return ""
	}

	var sections []string
	sections = append(sections, m.renderSnapshotList())
	sections = append(sections, m.renderSeparator())
	sections = append(sections, m.diffViewport.View())

	if m.searchActive {
		sections = append(sections, m.renderSearchBar())
	} else {
		sections = append(sections, m.renderStatusBar())
	}

	return strings.Join(sections, "\n")
}

func (m tuiModel) renderSnapshotList() string {
	var lines []string
	n := m.listLen()

	for i := m.listOffset; i < n && len(lines) < m.topHeight; i++ {
		idx := m.realIndex(i)
		snap := m.snapshots[idx]

		hash := shortHash(snap.Hash)
		date := snap.CreatedAt.Format("01-02 15:04")
		msg := snap.Message
		summary := changeSummaryFor(m.store, &snap)

		// Build line content
		var line strings.Builder
		if i == m.cursor {
			line.WriteString("> ")
		} else {
			line.WriteString("  ")
		}
		fmt.Fprintf(&line, "%s  %s  %-12s%s", hash, date, msg, summary)

		if names, ok := m.checkpoints[snap.ID]; ok {
			line.WriteString("  (")
			line.WriteString(strings.Join(names, ", "))
			line.WriteString(")")
		}

		if m.headID != nil && snap.ID == *m.headID {
			line.WriteString("  ")
			line.WriteString(hereStyle.Render("<- HERE"))
		}

		content := line.String()

		// Pad to width (use lipgloss.Width for ANSI-aware measurement)
		visWidth := lipgloss.Width(content)
		if visWidth < m.width {
			content += strings.Repeat(" ", m.width-visWidth)
		}

		if i == m.cursor {
			content = selectedStyle.Render(content)
		}

		lines = append(lines, content)
	}

	// Pad remaining lines
	for len(lines) < m.topHeight {
		lines = append(lines, strings.Repeat(" ", m.width))
	}

	return strings.Join(lines, "\n")
}

func (m tuiModel) renderSeparator() string {
	var label string
	if m.diffMode == vsFilesystem {
		label = " vs filesystem "
	} else {
		label = " vs parent "
	}

	focusHint := ""
	if m.focus == bottomPane {
		focusHint = " (j/k to scroll)"
	}

	filterHint := ""
	if m.filterQuery != "" {
		filterHint = fmt.Sprintf(" [filter: %s]", m.filterQuery)
	}

	left := "──" + label + "──"
	right := filterHint + focusHint
	padLen := m.width - len(left) - len(right)
	if padLen < 0 {
		padLen = 0
	}

	return separatorStyle.Render(left + strings.Repeat("─", padLen) + right)
}

func (m tuiModel) renderStatusBar() string {
	if m.diffLoading {
		return statusStyle.Render(pad("  Computing diff...", m.width))
	}

	var parts []string
	if m.focus == topPane {
		parts = append(parts, "j/k:navigate", "enter/tab:focus diff", "t:toggle mode", "/:search", "q:quit")
	} else {
		parts = append(parts, "j/k:scroll", "tab:focus list", "t:toggle mode", "/:search", "q:quit")
	}

	text := "  " + strings.Join(parts, "  ")
	return statusStyle.Render(pad(text, m.width))
}

func (m tuiModel) renderSearchBar() string {
	label := searchLabelStyle.Render("/")
	input := m.searchInput.View()
	line := label + input
	padLen := m.width - lipgloss.Width(line)
	if padLen > 0 {
		line += strings.Repeat(" ", padLen)
	}
	return line
}

// pad ensures s is exactly width characters (truncate or space-pad).
func pad(s string, width int) string {
	w := lipgloss.Width(s)
	if w >= width {
		// Truncate by runes (rough)
		runes := []rune(s)
		if len(runes) > width {
			return string(runes[:width])
		}
		return s
	}
	return s + strings.Repeat(" ", width-w)
}
