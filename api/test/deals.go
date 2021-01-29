package test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	files "github.com/ipfs/go-ipfs-files"
	"github.com/ipld/go-car"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-fil-markets/storagemarket"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/types"
	sealing "github.com/filecoin-project/lotus/extern/storage-sealing"
	"github.com/filecoin-project/lotus/node/impl"
	ipld "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
	dstest "github.com/ipfs/go-merkledag/test"
	unixfile "github.com/ipfs/go-unixfs/file"
)

func TestDealFlow(t *testing.T, b APIBuilder, blocktime time.Duration, carExport, fastRet bool, startEpoch abi.ChainEpoch) {
	s := setupOneClientOneMiner(t, b, blocktime)
	defer s.blockMiner.Stop()

	MakeDeal(t, s.ctx, 6, s.client, s.miner, carExport, fastRet, startEpoch)
}

func TestDoubleDealFlow(t *testing.T, b APIBuilder, blocktime time.Duration, startEpoch abi.ChainEpoch) {
	s := setupOneClientOneMiner(t, b, blocktime)
	defer s.blockMiner.Stop()

	MakeDeal(t, s.ctx, 6, s.client, s.miner, false, false, startEpoch)
	MakeDeal(t, s.ctx, 7, s.client, s.miner, false, false, startEpoch)
}

func MakeDeal(t *testing.T, ctx context.Context, rseed int, client api.FullNode, miner TestStorageNode, carExport, fastRet bool, startEpoch abi.ChainEpoch) {
	//res, data, err := CreateClientFile(ctx, client, rseed)
	res, _, err := CreateClientFile(ctx, client, rseed)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Print("\n\n\n\n\n\n\n\n")

	fcid := res.Root
	fmt.Println("FILE CID: ", fcid)

	dataCIDSize, err := client.ClientDealPieceCID(ctx, fcid)
	require.NoError(t, err)
	fmt.Println("File payload size: ", dataCIDSize.PayloadSize)

	deal := startDeal(t, ctx, miner, client, fcid, fastRet, startEpoch)

	// TODO: this sleep is only necessary because deals don't immediately get logged in the dealstore, we should fix this
	time.Sleep(time.Second)
	waitDealSealed(t, ctx, miner, client, deal, false)

	//// Retrieval
	//info, err := client.ClientGetDealInfo(ctx, *deal)
	//require.NoError(t, err)
	//
	//testRetrieval(t, ctx, client, fcid, &info.PieceCID, carExport, data)
}

func CreateClientFile(ctx context.Context, client api.FullNode, rseed int) (*api.ImportRes, []byte, error) {
	//data := make([]byte, 1600)
	data := make([]byte, 5000000)
	rand.New(rand.NewSource(int64(rseed))).Read(data)

	dir, err := ioutil.TempDir(os.TempDir(), "test-make-deal-")
	if err != nil {
		return nil, nil, err
	}

	path := filepath.Join(dir, "sourcefile.dat")
	err = ioutil.WriteFile(path, data, 0644)
	if err != nil {
		return nil, nil, err
	}

	res, err := client.ClientImport(ctx, api.FileRef{Path: path})
	if err != nil {
		return nil, nil, err
	}
	return res, data, nil
}

func TestFastRetrievalDealFlow(t *testing.T, b APIBuilder, blocktime time.Duration, startEpoch abi.ChainEpoch) {
	s := setupOneClientOneMiner(t, b, blocktime)
	defer s.blockMiner.Stop()

	data := make([]byte, 1600)
	rand.New(rand.NewSource(int64(8))).Read(data)

	r := bytes.NewReader(data)
	fcid, err := s.client.ClientImportLocal(s.ctx, r)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("FILE CID: ", fcid)

	deal := startDeal(t, s.ctx, s.miner, s.client, fcid, true, startEpoch)

	waitDealPublished(t, s.ctx, s.miner, deal)
	fmt.Println("deal published, retrieving")
	// Retrieval
	info, err := s.client.ClientGetDealInfo(s.ctx, *deal)
	require.NoError(t, err)

	testRetrieval(t, s.ctx, s.client, fcid, &info.PieceCID, false, data)
}

func TestSecondDealRetrieval(t *testing.T, b APIBuilder, blocktime time.Duration) {
	s := setupOneClientOneMiner(t, b, blocktime)
	defer s.blockMiner.Stop()

	{
		data1 := make([]byte, 800)
		rand.New(rand.NewSource(int64(3))).Read(data1)
		r := bytes.NewReader(data1)

		fcid1, err := s.client.ClientImportLocal(s.ctx, r)
		if err != nil {
			t.Fatal(err)
		}

		data2 := make([]byte, 800)
		rand.New(rand.NewSource(int64(9))).Read(data2)
		r2 := bytes.NewReader(data2)

		fcid2, err := s.client.ClientImportLocal(s.ctx, r2)
		if err != nil {
			t.Fatal(err)
		}

		deal1 := startDeal(t, s.ctx, s.miner, s.client, fcid1, true, 0)

		// TODO: this sleep is only necessary because deals don't immediately get logged in the dealstore, we should fix this
		time.Sleep(time.Second)
		waitDealSealed(t, s.ctx, s.miner, s.client, deal1, true)

		deal2 := startDeal(t, s.ctx, s.miner, s.client, fcid2, true, 0)

		time.Sleep(time.Second)
		waitDealSealed(t, s.ctx, s.miner, s.client, deal2, false)

		// Retrieval
		info, err := s.client.ClientGetDealInfo(s.ctx, *deal2)
		require.NoError(t, err)

		rf, _ := s.miner.SectorsRefs(s.ctx)
		fmt.Printf("refs: %+v\n", rf)

		testRetrieval(t, s.ctx, s.client, fcid2, &info.PieceCID, false, data2)
	}
}

func startDeal(t *testing.T, ctx context.Context, miner TestStorageNode, client api.FullNode, fcid cid.Cid, fastRet bool, startEpoch abi.ChainEpoch) *cid.Cid {
	maddr, err := miner.ActorAddress(ctx)
	if err != nil {
		t.Fatal(err)
	}

	addr, err := client.WalletDefaultAddress(ctx)
	if err != nil {
		t.Fatal(err)
	}
	deal, err := client.ClientStartDeal(ctx, &api.StartDealParams{
		Data: &storagemarket.DataRef{
			TransferType: storagemarket.TTGraphsync,
			Root:         fcid,
		},
		Wallet:            addr,
		Miner:             maddr,
		EpochPrice:        types.NewInt(3906250),
		DealStartEpoch:    startEpoch,
		MinBlocksDuration: uint64(build.MinDealDuration),
		FastRetrieval:     fastRet,
	})
	if err != nil {
		t.Fatalf("%+v", err)
	}

	return deal
}

func waitDealSealed(t *testing.T, ctx context.Context, miner TestStorageNode, client api.FullNode, deal *cid.Cid, noseal bool) {
loop:
	for {
		di, err := client.ClientGetDealInfo(ctx, *deal)
		if err != nil {
			t.Fatal(err)
		}
		switch di.State {
		case storagemarket.StorageDealAwaitingPreCommit, storagemarket.StorageDealSealing:
			if noseal {
				return
			}
			startSealingWaiting(t, ctx, miner)
		case storagemarket.StorageDealProposalRejected:
			t.Fatal("deal rejected")
		case storagemarket.StorageDealFailing:
			t.Fatal("deal failed")
		case storagemarket.StorageDealError:
			t.Fatal("deal errored", di.Message)
		case storagemarket.StorageDealActive:
			fmt.Println("COMPLETE", di)
			break loop
		}
		fmt.Println("Deal state: ", storagemarket.DealStates[di.State])
		time.Sleep(time.Second / 2)
	}
}

func waitDealPublished(t *testing.T, ctx context.Context, miner TestStorageNode, deal *cid.Cid) {
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	updates, err := miner.MarketGetDealUpdates(subCtx)
	if err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case <-ctx.Done():
			t.Fatal("context timeout")
		case di := <-updates:
			if deal.Equals(di.ProposalCid) {
				switch di.State {
				case storagemarket.StorageDealProposalRejected:
					t.Fatal("deal rejected")
				case storagemarket.StorageDealFailing:
					t.Fatal("deal failed")
				case storagemarket.StorageDealError:
					t.Fatal("deal errored", di.Message)
				case storagemarket.StorageDealFinalizing, storagemarket.StorageDealAwaitingPreCommit, storagemarket.StorageDealSealing, storagemarket.StorageDealActive:
					fmt.Println("COMPLETE", di)
					return
				}
				fmt.Println("Deal state: ", storagemarket.DealStates[di.State])
			}
		}
	}
}

func startSealingWaiting(t *testing.T, ctx context.Context, miner TestStorageNode) {
	snums, err := miner.SectorsList(ctx)
	require.NoError(t, err)

	for _, snum := range snums {
		si, err := miner.SectorsStatus(ctx, snum, false)
		require.NoError(t, err)

		t.Logf("Sector state: %s", si.State)
		if si.State == api.SectorState(sealing.WaitDeals) {
			require.NoError(t, miner.SectorStartSealing(ctx, snum))
		}
	}
}

func testRetrieval(t *testing.T, ctx context.Context, client api.FullNode, fcid cid.Cid, piece *cid.Cid, carExport bool, data []byte) {
	offers, err := client.ClientFindData(ctx, fcid, piece)
	if err != nil {
		t.Fatal(err)
	}

	if len(offers) < 1 {
		t.Fatal("no offers")
	}

	rpath, err := ioutil.TempDir("", "lotus-retrieve-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rpath) //nolint:errcheck

	caddr, err := client.WalletDefaultAddress(ctx)
	if err != nil {
		t.Fatal(err)
	}

	ref := &api.FileRef{
		Path:  filepath.Join(rpath, "ret"),
		IsCAR: carExport,
	}
	updates, err := client.ClientRetrieveWithEvents(ctx, offers[0].Order(caddr), ref)
	if err != nil {
		t.Fatal(err)
	}
	for update := range updates {
		if update.Err != "" {
			t.Fatalf("retrieval failed: %s", update.Err)
		}
	}

	rdata, err := ioutil.ReadFile(filepath.Join(rpath, "ret"))
	if err != nil {
		t.Fatal(err)
	}

	if carExport {
		rdata = extractCarData(t, ctx, rdata, rpath)
	}

	if !bytes.Equal(rdata, data) {
		t.Fatal("wrong data retrieved")
	}
}

func extractCarData(t *testing.T, ctx context.Context, rdata []byte, rpath string) []byte {
	bserv := dstest.Bserv()
	ch, err := car.LoadCar(bserv.Blockstore(), bytes.NewReader(rdata))
	if err != nil {
		t.Fatal(err)
	}
	b, err := bserv.GetBlock(ctx, ch.Roots[0])
	if err != nil {
		t.Fatal(err)
	}
	nd, err := ipld.Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	dserv := dag.NewDAGService(bserv)
	fil, err := unixfile.NewUnixfsFile(ctx, dserv, nd)
	if err != nil {
		t.Fatal(err)
	}
	outPath := filepath.Join(rpath, "retLoadedCAR")
	if err := files.WriteTo(fil, outPath); err != nil {
		t.Fatal(err)
	}
	rdata, err = ioutil.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	return rdata
}

type dealsScaffold struct {
	ctx        context.Context
	client     *impl.FullNodeAPI
	miner      TestStorageNode
	blockMiner *BlockMiner
}

func setupOneClientOneMiner(t *testing.T, b APIBuilder, blocktime time.Duration) *dealsScaffold {
	n, sn := b(t, OneFull, OneMiner)
	client := n[0].FullNode.(*impl.FullNodeAPI)
	miner := sn[0]
	return connectAndStartMining(t, b, blocktime, client, miner)
}

func connectAndStartMining(t *testing.T, b APIBuilder, blocktime time.Duration, client *impl.FullNodeAPI, miner TestStorageNode) *dealsScaffold {
	ctx := context.Background()
	addrinfo, err := client.NetAddrsListen(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if err := miner.NetConnect(ctx, addrinfo); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)

	blockMiner := NewBlockMiner(ctx, t, miner, blocktime)
	blockMiner.MineBlocks()

	return &dealsScaffold{
		ctx:        ctx,
		client:     client,
		miner:      miner,
		blockMiner: blockMiner,
	}
}
