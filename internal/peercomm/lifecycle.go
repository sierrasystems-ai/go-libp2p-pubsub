package peercomm

// beginRetirement establishes the submission barrier while the registry lock is
// held. No new event can be accepted once it returns true, and Registry.For can
// only observe either this actor before the barrier or a fresh actor afterward.
func (r *Registry) beginRetirement(a *Actor) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	a.submitMu.Lock()
	defer a.submitMu.Unlock()
	if a.retiring || a.pending != 0 || r.peers[a.peer] != a {
		return false
	}
	a.retiring = true
	delete(r.peers, a.peer)
	a.queue.close()
	return true
}

func (a *Actor) consumed() {
	a.submitMu.Lock()
	a.pending--
	a.submitMu.Unlock()
}
