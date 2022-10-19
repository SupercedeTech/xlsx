{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}
{-# LANGUAGE TupleSections     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE CPP #-}

module StreamTests
  ( tests
  ) where

#ifdef USE_MICROLENS

import Test.Tasty (TestName, TestTree, testGroup)
tests :: TestTree
tests = testGroup
  "I stubbed out the tests module for microlens \
   because it doesn't understand setOf. \
   Volunteers are welcome to fix this!"
    []
#else

import Control.Exception
import Codec.Xlsx
import Codec.Xlsx.Parser.Stream
import Codec.Xlsx.Types.Common
import Codec.Xlsx.Types.Internal.SharedStringTable
import Conduit ((.|))
import qualified Conduit as C
import Control.Exception (bracket)
import Control.Lens hiding (indexed)
import Data.Set.Lens
import Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy as LB
import qualified Data.ByteString as BS
import Data.IORef (modifyIORef', newIORef, readIORef)
import Data.Map (Map)
import qualified Data.Map as M
import qualified Data.Map as Map
import qualified Data.IntMap.Strict as IM
import Data.Maybe (mapMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text as Text
import Data.Vector (Vector, indexed, toList)
import Diff
import System.Directory (getTemporaryDirectory)
import System.Mem (disableAllocationLimit, enableAllocationLimit, setAllocationCounter)
import System.FilePath.Posix
import Test.Tasty (TestName, TestTree, testGroup)
import Test.Tasty.HUnit (testCase)
import TestXlsx
import Text.RawString.QQ
import Text.XML
import qualified Codec.Xlsx.Writer.Stream as SW
import qualified Codec.Xlsx.Writer.Internal.Stream as SW
import Control.Monad.State.Lazy
import Test.Tasty.SmallCheck
import qualified Data.Set as Set
import Data.Set (Set)
import Text.Printf
import Debug.Trace
import Control.DeepSeq
import Data.Conduit
import Codec.Xlsx.Formatted

toBs = LB.toStrict . fromXlsx testTime

tests :: TestTree
tests =
  testGroup "Stream tests"
    [
      testGroup "Writer/shared strings"
      [ testProperty "Input same as the output" sharedStringInputSameAsOutput
      , testProperty "Set of input texts is same as map length" sharedStringInputTextsIsSameAsMapLength
      , testProperty "Set of input texts is as value set length" sharedStringInputTextsIsSameAsValueSetLength
      ],

      testGroup "Reader/partial reading"
      [ testCase "Read part of a large workbook, don't hit allocation limit" $ readPartial bigWorkbook
      ],

      testGroup "Reader/Writer"
      [ testCase "Write as stream, see if memory based implementation can read it" $ readWrite simpleWorkbook
      , testCase "Write as stream, see if memory based implementation can read it" $ readWrite simpleWorkbookRow
      , testCase "Test a small workbook which has a fullblown sqaure" $ readWrite smallWorkbook
      , testCase "Test a big workbook as a full square which caused issues with zipstream \
                 The buffer of zipstream maybe 1kb, this workbook is big enough \
                 to be more than that. \
                 So if this encodes/decodes we know we can handle those sizes. \
                 In some older version the bytestring got cut off resulting in a corrupt xlsx file"
                  $ readWrite bigWorkbook
      -- , testCase "Write as stream, see if memory based implementation can read it" $ readWrite testXlsx
      -- TODO forall SheetItem write that can be read
      ],

      testGroup "Reader/inline strings"
      [ testCase "Can parse row with inline strings" inlineStringsAreParsed
      ]
    ]

readWrite :: Xlsx -> IO ()
readWrite input = do
  BS.writeFile "testinput.xlsx" (toBs input)
  items <- fmap (toListOf (traversed . si_row)) $ runXlsxM "testinput.xlsx" $ collectItems $ makeIndex 1
  bs <- runConduitRes $ void (SW.writeXlsx SW.defaultSettings $ C.yieldMany items) .| C.foldC
  case toXlsxEither $ LB.fromStrict bs of
    Right result  ->
      input @==?  result
    Left x -> do
      throwIO x

readPartial :: Xlsx -> IO ()
readPartial input = do
  BS.writeFile "testinput.xlsx" (toBs input)

  -- This constrains roughly how much memory the thread can allocate
  -- to 10MB (i.e. not enough to create all the 'SheetItem's for reading
  -- a decently-sized workbook.)
  setAllocationCounter 10_000_000
  enableAllocationLimit

  -- When we read the sheet normally, we allocate all the 'SheetItem's,
  -- even if they're not used. This hits the limit.
  eiException <- try $ void $ runXlsxM "testinput.xlsx" $ readSheet (makeIndex 1) (const $ pure ())
  case eiException of
    Right () -> error "Should have hit the allocation limit when reading the whole sheet"
    Left AllocationLimitExceeded -> pure ()

  setAllocationCounter 10_000_000

  -- When we read the sheet partially, we can prevent the allocation of
  -- 'SheetItem's after a point by saying that we're done with the read,
  -- using 'Stop'
  ioRef <- newIORef 0
  void $ runXlsxM "testinput.xlsx" $ readPartialSheet (makeIndex 1) $ \_sheetItem -> do
    curCount <- readIORef ioRef
    if curCount >= 100
      then pure Stop
      else modifyIORef' ioRef (+1) >> pure Continue

  setAllocationCounter 10_000_000

  -- But we can still read too much with 'readPartialSheet', if we don't stop
  -- early
  eiException <- try $ void $ runXlsxM "testinput.xlsx" $ readPartialSheet (makeIndex 1) (const $ pure Continue)
  case eiException of
    Right () -> error "Should have hit the allocation limit when reading the whole sheet"
    Left AllocationLimitExceeded -> pure ()

-- test if the input text is also the result (a property we use for convenience)
sharedStringInputSameAsOutput :: Text -> Either String String
sharedStringInputSameAsOutput someText =
  if someText  == out then Right msg  else Left msg
  where
    out = fst $ evalState (SW.upsertSharedString someText) SW.initialSharedString
    msg = printf "'%s' = '%s'" (Text.unpack out) (Text.unpack someText)

-- test if unique strings actually get set in the map as keys
sharedStringInputTextsIsSameAsMapLength :: [Text] -> Bool
sharedStringInputTextsIsSameAsMapLength someTexts =
    length result == length unqTexts
  where
   result  :: Map Text Int
   result = view SW.string_map $ traverse SW.upsertSharedString someTexts `execState` SW.initialSharedString
   unqTexts :: Set Text
   unqTexts = Set.fromList someTexts

-- test for every unique string we get a unique number
sharedStringInputTextsIsSameAsValueSetLength :: [Text] -> Bool
sharedStringInputTextsIsSameAsValueSetLength someTexts =
    length result == length unqTexts
  where
   result  :: Set Int
   result = setOf (SW.string_map . traversed) $ traverse SW.upsertSharedString someTexts `execState` SW.initialSharedString
   unqTexts :: Set Text
   unqTexts = Set.fromList someTexts

-- can we do xx
simpleWorkbook :: Xlsx
simpleWorkbook = set xlSheets sheets def
  where
    sheets = [("Sheet1" , toWs [((1,1), a1), ((1,2), cellValue ?~ CellText "text at B1 Sheet1" $ def)])]

a1 :: Cell
a1 = cellValue ?~ CellText "text at A1 Sheet1" $ cellStyle ?~ 1 $ def

-- can we do x
--           x
simpleWorkbookRow :: Xlsx
simpleWorkbookRow = set xlSheets sheets def
  where
    sheets = [("Sheet1" , toWs [((1,1), a1), ((2,1), cellValue ?~ CellText "text at A2 Sheet1" $ def)])]


tshow :: Show a => a -> Text
tshow = Text.pack . show

toWs :: [((Int,Int), Cell)] -> Worksheet
toWs x = set wsCells (M.fromList x) def

-- can we do xxx
--           xxx
--           .
--           .
smallWorkbook :: Xlsx
smallWorkbook = set xlSheets sheets def
  where
    sheets = [("Sheet1" , toWs $ [1..2] >>= \row ->
                  [((row,1), a1)
                  , ((row,2), def & cellValue ?~ CellText ("text at B"<> tshow row <> " Sheet1"))
                  , ((row,3), def & cellValue ?~ CellText "text at C1 Sheet1")
                  , ((row,4), def & cellValue ?~ CellDouble (0.2 + 0.1))
                  , ((row,5), def & cellValue ?~ CellBool False)
                  ]
              )]
bigWorkbook :: Xlsx
bigWorkbook = set xlSheets sheets def
  where
    sheets = [("Sheet1" , toWs $ [1..512] >>= \row ->
                  [((row,1), a1)
                  ,((row,2), def & cellValue ?~ CellText ("text at B"<> tshow row <> " Sheet1"))
                  ,((row,3), def & cellValue ?~ CellText "text at C1 Sheet1")
                  ]
              )]

inlineStringsAreParsed :: IO ()
inlineStringsAreParsed = do
  items <- runXlsxM "data/inline-strings.xlsx" $ collectItems $ makeIndex 1
  let expected =
        [ IM.fromList
            [ ( 1,
                Cell
                  { _cellStyle = Nothing,
                    _cellValue = Just (CellText "My Inline String"),
                    _cellComment = Nothing,
                    _cellFormula = Nothing
                  }
              ),
              ( 2,
                Cell
                  { _cellStyle = Nothing,
                    _cellValue = Just (CellText "two"),
                    _cellComment = Nothing,
                    _cellFormula = Nothing
                  }
              )
            ]
        ]
  expected @==? (items ^.. traversed . si_row . ri_cell_row)

#endif
